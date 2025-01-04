FROM golang:1.23 as base

WORKDIR /opt

COPY go.* .

RUN go mod download

COPY . .

RUN go build -ldflags="-s -w" -o standalone ./cmd/standalone

FROM debian:stable-slim as standalone

WORKDIR /

COPY --from=base /opt/standalone /

CMD ["/standalone"]
