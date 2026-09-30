// Package commitlog is a Kafka-style partition log: append-only segment files,
// a sparse offset index per segment and torn-write recovery.
//
// Record layout on disk (little endian):
//
//	len u32 | crc32c u32 | offset i64 | ts i64 | klen u16 | key | value
//
// len and crc cover everything after the crc field.
package commitlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	hdrSize      = 8         // len + crc
	fixedBody    = 8 + 8 + 2 // offset + ts + klen
	indexEvery   = 4 << 10   // one index entry per 4 KiB of log
	MaxRecord    = 1 << 20   // 1 MiB per record
	SegmentBytes = 128 << 20 // roll threshold
)

var (
	table         = crc32.MakeTable(crc32.Castagnoli)
	ErrCorrupt    = errors.New("commitlog: corrupt record")
	ErrOutOfRange = errors.New("commitlog: offset out of range")
	ErrTooLarge   = errors.New("commitlog: record too large")
)

type indexEntry struct {
	off int64 // absolute offset
	pos int64 // byte position in segment
}

type segment struct {
	base        int64
	next        int64 // next offset to assign
	size        int64
	maxTS       int64
	f           *os.File
	path        string
	index       []indexEntry
	lastIndexed int64
}

func segPath(dir string, base int64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d.log", base))
}

func idxPath(logPath string) string { return strings.TrimSuffix(logPath, ".log") + ".idx" }

// openSegment opens a segment. Sealed segments load their persisted index when it
// matches the file size; otherwise (and always for the active segment) the file is
// scanned, the sparse index rebuilt and a torn tail truncated.
func openSegment(dir string, base int64, sealed bool) (*segment, error) {
	p := segPath(dir, base)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	s := &segment{base: base, next: base, f: f, path: p, lastIndexed: -indexEvery}
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if sealed && s.loadIndex(st.Size()) {
		return s, nil
	}
	var pos int64
	hdr := make([]byte, hdrSize+fixedBody)
	body := make([]byte, 0, 64<<10)
	for pos < st.Size() {
		if _, err := f.ReadAt(hdr, pos); err != nil {
			break
		}
		n := int64(binary.LittleEndian.Uint32(hdr))
		if n < fixedBody || n > MaxRecord+fixedBody || pos+hdrSize+n > st.Size() {
			break
		}
		body = body[:n]
		if _, err := f.ReadAt(body, pos+hdrSize); err != nil || crc32.Checksum(body, table) != binary.LittleEndian.Uint32(hdr[4:]) {
			break
		}
		off := int64(binary.LittleEndian.Uint64(body))
		if off != s.next {
			break
		}
		s.noteIndex(off, pos)
		s.maxTS = max(s.maxTS, int64(binary.LittleEndian.Uint64(body[8:])))
		s.next++
		pos += hdrSize + n
	}
	if pos < st.Size() {
		if err := f.Truncate(pos); err != nil {
			return nil, err
		}
	}
	s.size = pos
	return s, nil
}

func (s *segment) noteIndex(off, pos int64) {
	if pos-s.lastIndexed >= indexEvery {
		s.index = append(s.index, indexEntry{off, pos})
		s.lastIndexed = pos
	}
}

// encode appends one record to dst.
func encode(dst []byte, off, ts int64, key, val []byte) []byte {
	n := fixedBody + len(key) + len(val)
	start := len(dst)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(n))
	dst = binary.LittleEndian.AppendUint32(dst, 0) // crc placeholder
	dst = binary.LittleEndian.AppendUint64(dst, uint64(off))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(ts))
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(key)))
	dst = append(dst, key...)
	dst = append(dst, val...)
	binary.LittleEndian.PutUint32(dst[start+4:], crc32.Checksum(dst[start+hdrSize:], table))
	return dst
}

// write appends pre-encoded records; positions are the start of each record in buf.
func (s *segment) write(buf []byte, firstOff int64, positions []int, ts int64) error {
	if _, err := s.f.WriteAt(buf, s.size); err != nil {
		return err
	}
	for i, p := range positions {
		s.noteIndex(firstOff+int64(i), s.size+int64(p))
	}
	s.size += int64(len(buf))
	s.next = firstOff + int64(len(positions))
	s.maxTS = max(s.maxTS, ts)
	return nil
}

// locate returns the byte position of offset off inside this segment.
func (s *segment) locate(off int64) (int64, error) {
	if off < s.base || off >= s.next {
		return 0, ErrOutOfRange
	}
	i := sort.Search(len(s.index), func(i int) bool { return s.index[i].off > off }) - 1
	pos, cur := int64(0), s.base
	if i >= 0 {
		pos, cur = s.index[i].pos, s.index[i].off
	}
	var lenBuf [4]byte
	for cur < off {
		if _, err := s.f.ReadAt(lenBuf[:], pos); err != nil {
			return 0, err
		}
		pos += hdrSize + int64(binary.LittleEndian.Uint32(lenBuf[:]))
		cur++
	}
	return pos, nil
}

// span returns how many bytes starting at pos hold whole records, up to maxBytes
// (at least one record, so a large record is never stuck).
func (s *segment) span(pos, maxBytes int64) (int64, int64, error) {
	var lenBuf [4]byte
	var n, count int64
	for pos+n < s.size {
		if _, err := s.f.ReadAt(lenBuf[:], pos+n); err != nil {
			if err == io.EOF {
				break
			}
			return 0, 0, err
		}
		rec := hdrSize + int64(binary.LittleEndian.Uint32(lenBuf[:]))
		if count > 0 && n+rec > maxBytes {
			break
		}
		n += rec
		count++
	}
	return n, count, nil
}

func (s *segment) close() error { return s.f.Close() }

// seal persists the sparse index so restarts skip scanning this segment.
// Layout: size i64 | next i64 | maxTS i64 | count u32 | (off i64, pos i64)*
func (s *segment) seal() error {
	b := make([]byte, 0, 28+16*len(s.index))
	b = binary.LittleEndian.AppendUint64(b, uint64(s.size))
	b = binary.LittleEndian.AppendUint64(b, uint64(s.next))
	b = binary.LittleEndian.AppendUint64(b, uint64(s.maxTS))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(s.index)))
	for _, e := range s.index {
		b = binary.LittleEndian.AppendUint64(b, uint64(e.off))
		b = binary.LittleEndian.AppendUint64(b, uint64(e.pos))
	}
	tmp := idxPath(s.path) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, idxPath(s.path))
}

func (s *segment) loadIndex(fileSize int64) bool {
	b, err := os.ReadFile(idxPath(s.path))
	if err != nil || len(b) < 28 {
		return false
	}
	size := int64(binary.LittleEndian.Uint64(b))
	n := int(binary.LittleEndian.Uint32(b[24:]))
	if size != fileSize || len(b) != 28+16*n {
		return false
	}
	s.size = size
	s.next = int64(binary.LittleEndian.Uint64(b[8:]))
	s.maxTS = int64(binary.LittleEndian.Uint64(b[16:]))
	s.index = make([]indexEntry, n)
	for i := range s.index {
		s.index[i] = indexEntry{int64(binary.LittleEndian.Uint64(b[28+16*i:])), int64(binary.LittleEndian.Uint64(b[36+16*i:]))}
	}
	return true
}

// Decode walks records in b (as returned by a fetch) and calls fn for each.
// key and val alias b.
func Decode(b []byte, fn func(off, ts int64, key, val []byte)) error {
	for len(b) > 0 {
		if len(b) < hdrSize+fixedBody {
			return ErrCorrupt
		}
		n := int(binary.LittleEndian.Uint32(b))
		if n < fixedBody || len(b) < hdrSize+n {
			return ErrCorrupt
		}
		body := b[hdrSize : hdrSize+n]
		if crc32.Checksum(body, table) != binary.LittleEndian.Uint32(b[4:]) {
			return ErrCorrupt
		}
		off := int64(binary.LittleEndian.Uint64(body))
		ts := int64(binary.LittleEndian.Uint64(body[8:]))
		kl := int(binary.LittleEndian.Uint16(body[16:]))
		if fixedBody+kl > n {
			return ErrCorrupt
		}
		fn(off, ts, body[fixedBody:fixedBody+kl], body[fixedBody+kl:])
		b = b[hdrSize+n:]
	}
	return nil
}
