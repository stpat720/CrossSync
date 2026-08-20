# syntax=docker/dockerfile:1
# CrossSync container for Unraid.
#
# The SQLite driver is pure Go, so the runtime image is a tiny static binary
# + an entrypoint that drops privileges to the PUID/PGID you set (Unraid
# convention, same as linuxserver.io containers: PUID=99 PGID=100 = nobody/users).
#
# Networking: run with --network=host so the container can dial peers at
# their Tailnet IPs and listen on the ports in config.yaml. (Or give it its
# own Tailscale interface and map the two ports.)

# ---- Stage 1: build the static Linux binary ----
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64
RUN go build -trimpath -ldflags="-s -w" -o /out/crosssync ./cmd/crosssync

# ---- Stage 2: runtime ----
FROM alpine:3.20
# shadow provides groupmod/groupadd/usermod/useradd, which the entrypoint
# needs to re-map PUID/PGID (Alpine's busybox does not ship them).
RUN apk add --no-cache ca-certificates tzdata su-exec shadow \
    && addgroup -S crosssync \
    && adduser -S -G crosssync crosssync

COPY --from=build /out/crosssync /usr/local/bin/crosssync
COPY deploy/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Unraid-style: 99 = nobody, 100 = users (change to match your shares' owner).
ENV PUID=99 \
    PGID=100 \
    TZ=Etc/UTC

# /config  = this node's config.yaml + meta_dir (appdata, bind-mounted)
# /data    = optional: a directory tree of user shares (or mount each share
#            directly at its own path, e.g. /mnt/user/<share> with --network
#            on Unraid those are already at /mnt/user/... inside the container)
VOLUME ["/config"]

# Default: serve with the config that lives in the appdata bind mount.
CMD ["serve", "--config", "/config/config.yaml"]

ENTRYPOINT ["/entrypoint.sh"]
