#!/bin/bash
# Lightweight watchdog for the claw-side data plane.
#
# autossh already re-dials a dropped SSH connection, and api-proxy is meant to
# run under a supervisor (systemd/nohup loop). This watchdog is a cheap backstop
# run from cron every minute: it only restarts things that are actually down,
# and unlike the legacy keepalive it never opens a second SSH connection to
# probe (which used to jam the target sshd) — all checks are local.
set -u

LOG="/tmp/tunnel-keepalive.log"
SCRIPTS="/root/.openclaw/workspace/scripts"
LOCAL_PROXY_PORT="__LOCAL_PROXY_PORT__"
REMOTE_API_PORT="__REMOTE_API_PORT__"

log() { echo "[$(date '+%F %T')] $*" >> "$LOG"; }

# 1. api-proxy must be listening locally; if not, the forward points at nothing.
if ! ss -tln 2>/dev/null | grep -q ":${LOCAL_PROXY_PORT} "; then
    log "api-proxy down on :${LOCAL_PROXY_PORT}, restarting"
    pkill -f "api-proxy.py" 2>/dev/null || true
    nohup python3 "$SCRIPTS/api-proxy.py" > /tmp/api-proxy.log 2>&1 &
    sleep 2
fi

# 2. a tunnel process must hold the reverse forward (autossh OR the plain-ssh
#    fallback loop both end up with an "ssh ... -R 127.0.0.1:PORT:" process).
if ! pgrep -f "ssh.*-R 127.0.0.1:${REMOTE_API_PORT}:" >/dev/null; then
    log "tunnel down for remote :${REMOTE_API_PORT}, restarting"
    pkill -f "reverse-tunnel.sh" 2>/dev/null || true
    pkill -f "ssh.*-R 127.0.0.1:${REMOTE_API_PORT}:" 2>/dev/null || true
    nohup bash "$SCRIPTS/reverse-tunnel.sh" > /tmp/reverse-tunnel.log 2>&1 &
fi
