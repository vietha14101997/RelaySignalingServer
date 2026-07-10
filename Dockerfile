# syntax=docker/dockerfile:1
# Multi-stage build for the RemoteScreen Go relay (pure Go, CGO-free).

FROM golang:1.26-alpine AS build
WORKDIR /src
# Cache module downloads first (only re-runs when go.mod/go.sum change).
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Static binary — no CGO needed (pgx, echo, gorilla are pure Go).
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/relay-server ./cmd/server

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget \
    && addgroup -S relay && adduser -S -G relay relay
COPY --from=build /out/relay-server /usr/local/bin/relay-server
USER relay
EXPOSE 8443
# Relay listens plain HTTP here; TLS is terminated by Caddy in front.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8443/health >/dev/null 2>&1 || exit 1
ENTRYPOINT ["/usr/local/bin/relay-server"]
