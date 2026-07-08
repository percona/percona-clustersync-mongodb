#!/usr/bin/env bash
# Stop and remove the HA group containers. The clusters are left running.
#
# Usage: ./hack/ha/stop.sh

set -euo pipefail

HA_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
COMPOSE="$HA_DIR/compose.yml"

# The external network name is required to parse the compose file, but is
# irrelevant for `down`; supply a placeholder so compose does not error.
export PCSM_HA_NETWORK="${PCSM_HA_NETWORK:-bridge}"
export PCSM_SOURCE_URI="${PCSM_SOURCE_URI:-}"
export PCSM_TARGET_URI="${PCSM_TARGET_URI:-}"

docker compose -f "$COMPOSE" down --remove-orphans

echo "HA group stopped."
