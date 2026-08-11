#!/usr/bin/env bash
# Failover drill: find the ACTIVE instance and hard-kill its container so a
# standby has to take over, then (by default) restart it after a short delay so
# it rejoins the group. The restarted container is a fresh process (new
# instanceId) and comes back as STANDBY (or re-wins after the lease expires).
#
# Usage: ./hack/ha/kill-active.sh [SECONDS] [--no-restart]
#   SECONDS       delay before restarting the killed instance (default 5)
#   --no-restart  leave the instance stopped
#
# Note on timing vs. the lease TTL (10s): a short delay (< TTL) means the killed
# instance may return before its lease expires; a longer delay lets a standby
# take over first. Both are valid drills.

set -euo pipefail

DELAY=5
RESTART=true

for arg in "$@"; do
    case "$arg" in
        --no-restart) RESTART=false ;;
        '' | *[!0-9]*)
            echo "Unknown argument: $arg" >&2
            echo "Usage: $0 [SECONDS] [--no-restart]" >&2
            exit 1
            ;;
        *) DELAY="$arg" ;;
    esac
done

# Parallel arrays (portable to macOS bash 3.2, which lacks associative arrays).
PORTS=(2242 2243 2244)
CONTAINERS=(pcsm0 pcsm1 pcsm2)

for i in "${!PORTS[@]}"; do
    port="${PORTS[$i]}"
    body=$(curl -s --max-time 2 "http://localhost:${port}/status" 2>/dev/null || true)
    [[ -z "$body" ]] && continue

    role=$(jq -r '.role // ""' <<<"$body" 2>/dev/null || echo "")
    if [[ "$role" == "ACTIVE" ]]; then
        c="${CONTAINERS[$i]}"
        id=$(jq -r '.me.instanceId // "?"' <<<"$body")
        echo "Killing ACTIVE: $c (port $port, $id)"
        docker kill -s KILL "$c" >/dev/null

        if [[ "$RESTART" != "true" ]]; then
            echo "Killed (left stopped). Watch failover: ./hack/ha/status-group.sh"
            exit 0
        fi

        echo "Killed. Restarting $c in ${DELAY}s..."
        sleep "$DELAY"
        docker start "$c" >/dev/null
        echo "Restarted $c. Watch the group: ./hack/ha/status-group.sh"
        exit 0
    fi
done

echo "No ACTIVE instance found among ports 2242-2244." >&2
exit 1
