#!/usr/bin/env bash
# Runs `dagger` with the given arguments. Retries when Dagger Cloud has no
# engine to give, which fails before any pipeline step starts, and when an
# image pull fails on a network error, where the engine's cache keeps the
# steps that already ran.
#
# Env: optional DAGGER_MAX_ATTEMPTS (default 5) and
# DAGGER_RETRY_BASE_SECONDS (default 30, doubles per retry).
set -uo pipefail

max=${DAGGER_MAX_ATTEMPTS:-5}
delay=${DAGGER_RETRY_BASE_SECONDS:-30}
log=$(mktemp)
trap 'rm -f "$log"' EXIT

attempt=1
while :; do
  dagger --progress plain "$@" 2>&1 | tee "$log"
  status=${PIPESTATUS[0]}
  if (( status == 0 || attempt >= max )); then
    break
  fi
  if grep -aq 'start engine: failed to provision a remote Engine' "$log"; then
    reason="No cloud engine"
  elif grep -aE 'pull image "[^"]+": ' "$log" \
    | grep -aqE 'connection reset by peer|i/o timeout|TLS handshake timeout|unexpected EOF|502 Bad Gateway|503 Service Unavailable|504 Gateway Time'; then
    reason="Image pull failed on a network error"
  else
    break
  fi
  echo "::warning title=dagger::$reason on attempt $attempt of $max; retrying in ${delay}s"
  sleep "$delay"
  delay=$((delay * 2))
  attempt=$((attempt + 1))
done
exit "$status"
