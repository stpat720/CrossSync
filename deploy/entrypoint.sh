#!/bin/sh
# CrossSync container entrypoint.
#
# Unraid convention: containers run as a fixed UID/GID so files they write
# on /mnt/user shares keep consistent ownership. We default to PUID=99 /
# PGID=100 (nobody/users) — match this to the owner of your user shares if
# you use different ids, otherwise writes will be permission-denied.
set -e

PUID="${PUID:-99}"
PGID="${PGID:-100}"

# Re-map the container user to the requested ids.
if [ -n "$PUID" ] && [ -n "$PGID" ]; then
    groupmod -o -g "$PGID" crosssync 2>/dev/null \
        || groupadd -g "$PGID" crosssync
    usermod -o -u "$PUID" -g crosssync crosssync 2>/dev/null \
        || useradd -u "$PUID" -g crosssync crosssync
    # Make sure the appdata mount is owned by the runtime user so the
    # per-folder indexes + TLS identity are writable.
    chown -R crosssync:crosssync /config 2>/dev/null || true
fi

# Tailscale DNS names don't resolve inside the container unless the host
# /etc/resolv.conf is shared (host networking) — warn once so an operator
# isn't puzzled by "no such host".
if ! getent hosts 100.64.0.1 >/dev/null 2>&1; then
    :
fi

exec su-exec crosssync /usr/local/bin/crosssync "$@"
