#!/usr/bin/env bash
# Print the full /status response of the ACTIVE instance (pretty JSON).
#
# Usage: ./hack/ha/status-active.sh

set -uo pipefail

PORTS=(2242 2243 2244)

for port in "${PORTS[@]}"; do
    body=$(curl -s --max-time 2 "http://localhost:${port}/status" 2>/dev/null || true)
    [[ -z "$body" ]] && continue

    role=$(jq -r '.role // ""' <<<"$body" 2>/dev/null || echo "")
    if [[ "$role" == "ACTIVE" ]]; then
        echo "ACTIVE on port $port:"
        jq . <<<"$body"
        exit 0
    fi
done

echo "No ACTIVE instance found among ports 2242-2244." >&2
exit 1
