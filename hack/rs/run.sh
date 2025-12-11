#!/usr/bin/env bash
# Start MongoDB replica sets for source and target
#
# Usage:
#   ./run.sh           # Default: start both rs0 and rs1
#   RS_COUNT=1 ./run.sh   # Start only rs0
#   RS_COUNT=2 ./run.sh   # Start both rs0 and rs1
#
# Limits: Max 2 replica sets (rs0 and rs1)
# Each replica set has 3 members (rs{N}0, rs{N}1, rs{N}2)
# Ports: rs0 uses 30000-30002, rs1 uses 30100-30102

export COMPOSE_PROJECT_NAME=r1

BASE=$(dirname "$(dirname "$0")")
echo "BASE=$BASE"

# shellcheck source=/dev/null
source "$BASE/util"

RDIR="$BASE/rs"

export compose="$RDIR/compose.yml"

# Port mappings for each replica set (must match compose.yml)
# rs0: 30000, 30001, 30002
# rs1: 30100, 30101, 30102
RS_PORTS=(
    "30000 30001 30002"
    "30100 30101 30102"
)

# Dynamically defined by the length of sets of ports
MAX_RS_COUNT=${#RS_PORTS[@]}

# Configuration: Number of replica sets to start
# Can be overridden with environment variable: RS_COUNT=1 ./run.sh
RS_COUNT="${RS_COUNT:-$MAX_RS_COUNT}"

if [ "$RS_COUNT" -gt "$MAX_RS_COUNT" ]; then
    echo "ERROR: RS_COUNT=$RS_COUNT exceeds maximum $MAX_RS_COUNT"
    echo "Available init scripts: $RDIR/mongo/scripts/rs{0..$((MAX_RS_COUNT-1))}.js"
    exit 1
fi

if [ "$RS_COUNT" -lt 1 ]; then
    echo "ERROR: RS_COUNT=$RS_COUNT must be at least 1"
    exit 1
fi

echo "Starting $RS_COUNT replica set(s) (max: $MAX_RS_COUNT)"

# Build list of services to start and initialize each replica set
SERVICES=""
for rs_idx in $(seq 0 $((RS_COUNT - 1))); do
    echo "Starting replica set rs${rs_idx}"
    SERVICES="$SERVICES rs${rs_idx}0 rs${rs_idx}1 rs${rs_idx}2"
done

# Start the selected services
dcf up -d $SERVICES

# Initialize each replica set
for rs_idx in $(seq 0 $((RS_COUNT - 1))); do
    # Get ports for this replica set
    read -r -a PORTS <<< "${RS_PORTS[$rs_idx]}"

    # Wait for all members
    for member_idx in 0 1 2; do
        PORT=${PORTS[$member_idx]}
        mwait "rs${rs_idx}${member_idx}:${PORT}"
    done

    # Initialize replica set
    rsinit "scripts/rs${rs_idx}" "rs${rs_idx}0:${PORTS[0]}"
done
