FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/logbroker ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/logbroker /logbroker
VOLUME /data
EXPOSE 9092
ENTRYPOINT ["/logbroker", "-addr", ":9092", "-data", "/data"]
