#!/usr/bin/env bash
# Start MongoDB sharded clusters for source and target
#
# Usage:
#   ./run.sh                      # Default: 2 source shards, 2 target shards
#   SRC_SHARDS=1 ./run.sh         # Custom source shards, default target
#   TGT_SHARDS=3 ./run.sh         # Default source, custom target shards
#   SRC_SHARDS=3 TGT_SHARDS=1 ./run.sh  # Custom both
#   SRC_SHARDS=0 TGT_SHARDS=2 ./run.sh  # Target only
#   SRC_SHARDS=2 TGT_SHARDS=0 ./run.sh  # Source only
#
# Limits: Max 3 shards each for source and target (0 = skip source/target cluster)
# Services are named: src-rs{N}0 and tgt-rs{N}0 where N is the shard index (0-based)
# Ports: source shards start at 30000 (increment by 100), target shards at 40000

export COMPOSE_PROJECT_NAME=s1

BASE=$(dirname "$(dirname "$0")")
echo "BASE=$BASE"

# shellcheck source=/dev/null
source "$BASE/util"

SDIR="$BASE/sh"

export compose=$SDIR/compose.yml

# Configuration: Number of shards for source and target clusters
# Can be overridden with environment variables: SRC_SHARDS=2 TGT_SHARDS=1 ./run.sh
SRC_SHARDS="${SRC_SHARDS:-2}"
TGT_SHARDS="${TGT_SHARDS:-2}"

# Port mappings for each shard (must match compose.yml)
# Source shards: rs0=30000, rs1=30100, rs2=30200
# Target shards: rs0=40000, rs1=40100, rs2=40200
SRC_SHARD_PORTS=(30000 30100 30200)
TGT_SHARD_PORTS=(40000 40100 40200)

MAX_SRC_SHARDS=${#SRC_SHARD_PORTS[@]}
MAX_TGT_SHARDS=${#TGT_SHARD_PORTS[@]}

if [ "$SRC_SHARDS" -gt "$MAX_SRC_SHARDS" ]; then
    echo "ERROR: SRC_SHARDS=$SRC_SHARDS exceeds maximum $MAX_SRC_SHARDS"
    echo "Available init scripts: $SDIR/mongo/src/rs{0..$((MAX_SRC_SHARDS-1))}.js"
    exit 1
fi

if [ "$TGT_SHARDS" -gt "$MAX_TGT_SHARDS" ]; then
    echo "ERROR: TGT_SHARDS=$TGT_SHARDS exceeds maximum $MAX_TGT_SHARDS"
    echo "Available init scripts: $SDIR/mongo/tgt/rs{0..$((MAX_TGT_SHARDS-1))}.js"
    exit 1
fi



# Start source sharded cluster only if SRC_SHARDS > 0
if [ "$SRC_SHARDS" -gt 0 ]; then
    echo "Starting source cluster with $SRC_SHARDS shards (max: $MAX_SRC_SHARDS)"

    # Build list of source shard services to start
    SRC_SERVICES="src-cfg0"
    for i in $(seq 0 $((SRC_SHARDS - 1))); do
        SRC_SERVICES="$SRC_SERVICES src-rs${i}0"
    done

    # Start source config server and shards
    dcf up -d $SRC_SERVICES

    # Initialize source config server
    mwait "src-cfg0:27000" && rsinit "src/cfg" "src-cfg0:27000"

    # Initialize source shards
    for i in $(seq 0 $((SRC_SHARDS - 1))); do
        PORT=${SRC_SHARD_PORTS[$i]}
        mwait "src-rs${i}0:${PORT}" && rsinit "src/rs${i}" "src-rs${i}0:${PORT}"
    done

    # Start source mongos
    dcf up -d src-mongos && mwait "src-mongos:27017"

    # Add source shards to cluster
    ADD_SHARDS_CMD=""
    for i in $(seq 0 $((SRC_SHARDS - 1))); do
        PORT=${SRC_SHARD_PORTS[$i]}
        ADD_SHARDS_CMD="${ADD_SHARDS_CMD}sh.addShard('rs${i}/src-rs${i}0:${PORT}'); "
    done
    msh "src-mongos:27017" --eval "$ADD_SHARDS_CMD"
else
    echo "Skipping source cluster (SRC_SHARDS=0)"
fi

# Start target sharded cluster only if TGT_SHARDS > 0
if [ "$TGT_SHARDS" -gt 0 ]; then
    echo "Starting target cluster with $TGT_SHARDS shards (max: $MAX_TGT_SHARDS)"

    # Build list of target shard services to start
    TGT_SERVICES="tgt-cfg0"
    for i in $(seq 0 $((TGT_SHARDS - 1))); do
        TGT_SERVICES="$TGT_SERVICES tgt-rs${i}0"
    done

    # Start target config server and shards
    dcf up -d $TGT_SERVICES

    # Initialize target config server
    mwait "tgt-cfg0:28000" && rsinit "tgt/cfg" "tgt-cfg0:28000"

    # Initialize target shards
    for i in $(seq 0 $((TGT_SHARDS - 1))); do
        PORT=${TGT_SHARD_PORTS[$i]}
        mwait "tgt-rs${i}0:${PORT}" && rsinit "tgt/rs${i}" "tgt-rs${i}0:${PORT}"
    done

    # Start target mongos
    dcf up -d tgt-mongos && mwait "tgt-mongos:27017"

    # Add target shards to cluster
    ADD_SHARDS_CMD=""
    for i in $(seq 0 $((TGT_SHARDS - 1))); do
        PORT=${TGT_SHARD_PORTS[$i]}
        ADD_SHARDS_CMD="${ADD_SHARDS_CMD}sh.addShard('rs${i}/tgt-rs${i}0:${PORT}'); "
    done
    msh "tgt-mongos:27017" --eval "$ADD_SHARDS_CMD"
else
    echo "Skipping target cluster (TGT_SHARDS=0)"
fi
