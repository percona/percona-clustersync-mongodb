#!/usr/bin/env bash
# Start a 3-node PCSM HA group (pcsm0/1/2) against already-running clusters.
#
# Usage: ./hack/ha/run.sh [rs|sh] [--reset]
#   rs        target the RS topology     (default)
#   sh        target the sharded topology
#   --reset   clear target state (lease/members/checkpoints) before starting
#
# Prerequisites: start the clusters first (hack/rs/run.sh or hack/sh/run.sh).
#
# The instances join the cluster's Docker network and reach it via the same
# hostnames the host uses (rs00, src-mongos, ...). API ports 2242-2244 are
# published on host loopback, while Prometheus scrapes them over pcsm-metrics.

set -euo pipefail

HA_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HA_DIR/../.." && pwd)
COMPOSE="$HA_DIR/compose.yml"

TOPO="rs"
RESET="false"

for arg in "$@"; do
    case "$arg" in
        rs | sh) TOPO="$arg" ;;
        --reset) RESET="true" ;;
        *)
            echo "Unknown argument: $arg" >&2
            echo "Usage: $0 [rs|sh] [--reset]" >&2
            exit 1
            ;;
    esac
done

case "$TOPO" in
    rs)
        NETWORK="rs_default"
        SOURCE_URI="${PCSM_SOURCE_URI:-mongodb://rs00:30000}"
        TARGET_URI="${PCSM_TARGET_URI:-mongodb://rs10:30100}"
        ;;
    sh)
        NETWORK="s1_default"
        SOURCE_URI="${PCSM_SOURCE_URI:-mongodb://src-mongos:27017}"
        TARGET_URI="${PCSM_TARGET_URI:-mongodb://tgt-mongos:27017}"
        ;;
esac

if ! docker network inspect "$NETWORK" >/dev/null 2>&1; then
    echo "Docker network '$NETWORK' not found." >&2
    echo "Start the $TOPO clusters first: ./hack/$TOPO/run.sh" >&2
    exit 1
fi

METRICS_NETWORK="pcsm-metrics"
if ! docker network inspect "$METRICS_NETWORK" >/dev/null 2>&1; then
    echo "Creating shared metrics network '$METRICS_NETWORK'..."
    docker network create "$METRICS_NETWORK" >/dev/null
fi

export PCSM_HA_NETWORK="$NETWORK"
export PCSM_SOURCE_URI="$SOURCE_URI"
export PCSM_TARGET_URI="$TARGET_URI"

echo "Building pcsm:dev image..."
docker build -f "$HA_DIR/Dockerfile" -t pcsm:dev "$ROOT"

# Stop any existing group first. Otherwise a --reset would wipe the lease/state
# out from under running instances, leaving them with stale in-memory terms.
echo "Stopping any existing HA group..."
docker compose -f "$COMPOSE" down --remove-orphans 2>/dev/null || true

if [[ "$RESET" == "true" ]]; then
    echo "Resetting target state on $TARGET_URI..."
    docker run --rm --network "$NETWORK" pcsm:dev reset --target "$TARGET_URI"
fi

echo "Starting HA group (source=$SOURCE_URI target=$TARGET_URI network=$NETWORK metrics=$METRICS_NETWORK)..."
docker compose -f "$COMPOSE" up -d

echo
"$HA_DIR/status-group.sh" || true

echo
echo "Logs:   docker logs -f pcsm0             (or pcsm1 / pcsm2)"
echo "Group:  ./hack/ha/status-group.sh"
echo "Active: ./hack/ha/status-active.sh"
echo "Kill:   ./hack/ha/kill-active.sh         (failover drill)"
echo "Stop:   ./hack/ha/stop.sh"
