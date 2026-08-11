#!/usr/bin/env bash
# Print the HA group state: one row per instance from its /status envelope.
#
# Usage: ./hack/ha/status-group.sh

set -uo pipefail

PORTS=(2242 2243 2244)

fmt="%-7s %-28s %-12s %-9s %-11s %-5s\n"

# shellcheck disable=SC2059
printf "$fmt" "PORT" "INSTANCE_ID" "HOST" "ROLE" "STATE" "TERM"

for port in "${PORTS[@]}"; do
    body=$(curl -s --max-time 2 "http://localhost:${port}/status" 2>/dev/null || true)

    if [[ -z "$body" ]]; then
        # shellcheck disable=SC2059
        printf "$fmt" "$port" "-" "-" "DOWN" "-" "-"
        continue
    fi

    id=$(jq -r '.me.instanceId // "-"' <<<"$body" 2>/dev/null || echo "-")
    role=$(jq -r '.role // "-"' <<<"$body" 2>/dev/null || echo "-")
    state=$(jq -r '.state // "-"' <<<"$body" 2>/dev/null || echo "-")
    term=$(jq -r '.group.term // "-"' <<<"$body" 2>/dev/null || echo "-")

    # This instance's host is the member entry whose instanceId matches me.
    host=$(jq -r --arg id "$id" '(.group.members[]? | select(.instanceId == $id) | .host) // "-"' \
        <<<"$body" 2>/dev/null || echo "-")

    # shellcheck disable=SC2059
    printf "$fmt" "$port" "$id" "$host" "$role" "$state" "$term"
done
