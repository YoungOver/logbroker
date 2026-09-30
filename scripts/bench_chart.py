import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11})
fig, (a, b) = plt.subplots(1, 2, figsize=(12, 4.6), dpi=150)

labels = ["100 B msgs\nbatch 500", "1 KB msgs\nbatch 100", "1 KB msgs\nacks=all (fsync)"]
msgs = [1217, 573, 149]
bars = a.bar(labels, msgs, color=["#2a6f97", "#2a6f97", "#61a5c2"], width=0.55)
for bar, v, mb in zip(bars, msgs, [116, 559, 145]):
    a.text(bar.get_x() + bar.get_width() / 2, v + 25, f"{v}k msg/s\n{mb} MB/s", ha="center", fontsize=10)
a.set_ylabel("thousand messages / second")
a.set_ylim(0, 1450)
a.set_title("Produce while consuming, 8 partitions", loc="left", fontweight="bold")

cases = ["1 KB, acks=1", "1 KB, acks=all", "100 B, acks=1"]
p50 = [4.4, 4.1, 12.8]
p99 = [33.5, 12.5, 56.4]
x = range(len(cases))
b.barh([i + 0.2 for i in x], p50, height=0.38, color="#2a6f97", label="p50")
b.barh([i - 0.2 for i in x], p99, height=0.38, color="#b0b7bf", label="p99")
b.set_yticks(list(x), cases)
for i in x:
    b.text(p50[i] + 0.8, i + 0.2, f"{p50[i]} ms", va="center", fontsize=10)
    b.text(p99[i] + 0.8, i - 0.2, f"{p99[i]} ms", va="center", fontsize=10)
b.set_xlabel("end-to-end latency, produce to consumer (ms)")
b.set_xlim(0, 68)
b.legend(frameon=False, loc="lower right")
b.set_title("Latency under full load", loc="left", fontweight="bold")
for ax in (a, b):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
fig.text(0.01, 0.01, "i7-13620H, NVMe, Go 1.25, HTTP/1.1 on localhost. Restart with 9.4 GB of log: 2.4 s.", fontsize=9, color="#555")
plt.tight_layout(rect=(0, 0.04, 1, 1))
plt.savefig("docs/bench.png")
