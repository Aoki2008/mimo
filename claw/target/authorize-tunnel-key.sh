#!/bin/bash
# Forced-command authorizer (scheme B, minimal privilege).
#
# Runs AS the `tunnel` user, invoked by the panel's admin key which is wired in
# ~tunnel/.ssh/authorized_keys as:
#
#   command="/usr/local/bin/authorize-tunnel-key",restrict <panel-admin-pubkey>
#
# The panel calls it:
#   ssh -i panel_admin_key tunnel@target "<remote_api_port> ssh-ed25519 <blob> [comment]"
#
# It appends ONE locked line to tunnel's OWN authorized_keys so the authorized
# claw key can ONLY open that single reverse forward (permitlisten) — no shell,
# no other ports. The panel admin line is always preserved. No sudo/root needed:
# the file being edited is tunnel's own, and SSH_ORIGINAL_COMMAND is delivered
# straight by sshd (no sudo env stripping).
set -euo pipefail

AUTHK="$HOME/.ssh/authorized_keys"

read -r -a ARGS <<< "${SSH_ORIGINAL_COMMAND:-}"
PORT="${ARGS[0]:-}"; KEYTYPE="${ARGS[1]:-}"; KEYBLOB="${ARGS[2]:-}"; KEYCOMMENT="${ARGS[3]:-claw}"

case "$PORT" in ''|*[!0-9]*) echo "ERR: bad port"; exit 2 ;; esac
if [ "$PORT" -lt 1024 ] || [ "$PORT" -gt 65535 ]; then echo "ERR: port range"; exit 2; fi
if [ "$KEYTYPE" != "ssh-ed25519" ] || [ -z "$KEYBLOB" ]; then echo "ERR: need ssh-ed25519 key"; exit 2; fi
KEYCOMMENT="$(printf '%s' "$KEYCOMMENT" | tr -cd 'A-Za-z0-9_.@-')"

mkdir -p "$HOME/.ssh"; touch "$AUTHK"; chmod 700 "$HOME/.ssh"; chmod 600 "$AUTHK"

OPTS="restrict,permitlisten=\"127.0.0.1:${PORT}\",command=\"echo tunnel-only; sleep infinity\""
LINE="${OPTS} ${KEYTYPE} ${KEYBLOB} ${KEYCOMMENT}"

# Atomic rewrite: keep every existing line except a prior entry for THIS key
# blob (idempotent re-auth), then append the fresh one. The panel admin line
# has a different blob, so it is always preserved.
TMP="$(mktemp "$HOME/.ssh/.authk.XXXXXX")"
grep -vF "$KEYBLOB" "$AUTHK" > "$TMP" || true
printf '%s\n' "$LINE" >> "$TMP"
chmod 600 "$TMP"; mv -f "$TMP" "$AUTHK"
echo "OK authorized ${KEYCOMMENT} -> 127.0.0.1:${PORT}"
