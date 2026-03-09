#!/usr/bin/env bash
# Start MongoDB sharded clusters for source and/or target
#
# Usage:
#   ./run.sh                      # Start both source and target (default)
#   ./run.sh --source             # Start source cluster only
#   ./run.sh --target             # Start target cluster only
#   ./run.sh -s -t                # Start both (explicit)
#   SRC_SHARDS=1 ./run.sh         # Custom source shards, default target
#   TGT_SHARDS=3 ./run.sh         # Default source, custom target shards
#   SRC_SHARDS=3 TGT_SHARDS=1 ./run.sh  # Custom both
#
# Limits: Max 3 shards each for source and target
# Services are named: src-rs{N}0 and tgt-rs{N}0 where N is the shard index (0-based)
# Ports: source shards start at 30000 (increment by 100), target shards at 40000

export COMPOSE_PROJECT_NAME=s1

BASE=$(dirname "$(dirname "$0")")
echo "BASE=$BASE"

# shellcheck source=/dev/null
source "$BASE/util"

SDIR="$BASE/sh"

export compose=$SDIR/compose.yml

SOURCE=false
TARGET=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--source) SOURCE=true; shift ;;
        -t|--target) TARGET=true; shift ;;
        -h|--help)
            echo "Usage: $0 [--source|-s] [--target|-t]"
            echo "  No flags: start both source and target"
            echo "  -s, --source  Start source cluster only"
            echo "  -t, --target  Start target cluster only"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if ! $SOURCE && ! $TARGET; then
    SOURCE=true
    TARGET=true
fi

# Port mappings for each shard (must match compose.yml)
# Source shards: rs0=30000, rs1=30100, rs2=30200
# Target shards: rs0=40000, rs1=40100, rs2=40200
SRC_SHARD_PORTS=(30000 30100 30200)
TGT_SHARD_PORTS=(40000 40100 40200)

SRC_SHARDS="${SRC_SHARDS:-2}"
TGT_SHARDS="${TGT_SHARDS:-2}"

MAX_SRC_SHARDS=${#SRC_SHARD_PORTS[@]}
MAX_TGT_SHARDS=${#TGT_SHARD_PORTS[@]}

if $SOURCE && [ "$SRC_SHARDS" -gt "$MAX_SRC_SHARDS" ]; then
    echo "ERROR: SRC_SHARDS=$SRC_SHARDS exceeds maximum $MAX_SRC_SHARDS"
    echo "Available init scripts: $SDIR/mongo/src/rs{0..$((MAX_SRC_SHARDS-1))}.js"
    exit 1
fi

if $TARGET && [ "$TGT_SHARDS" -gt "$MAX_TGT_SHARDS" ]; then
    echo "ERROR: TGT_SHARDS=$TGT_SHARDS exceeds maximum $MAX_TGT_SHARDS"
    echo "Available init scripts: $SDIR/mongo/tgt/rs{0..$((MAX_TGT_SHARDS-1))}.js"
    exit 1
fi

if $SOURCE; then
    echo "Starting source cluster with $SRC_SHARDS shards (max: $MAX_SRC_SHARDS)"

    SRC_SERVICES="src-cfg0"
    for i in $(seq 0 $((SRC_SHARDS - 1))); do
        SRC_SERVICES="$SRC_SERVICES src-rs${i}0"
    done

    dcf up -d $SRC_SERVICES

    mwait "src-cfg0:27000" && rsinit "src/cfg" "src-cfg0:27000"

    for i in $(seq 0 $((SRC_SHARDS - 1))); do
        PORT=${SRC_SHARD_PORTS[$i]}
        mwait "src-rs${i}0:${PORT}" && rsinit "src/rs${i}" "src-rs${i}0:${PORT}"
    done

    dcf up -d src-mongos && mwait "src-mongos:27017"

    ADD_SHARDS_CMD=""
    for i in $(seq 0 $((SRC_SHARDS - 1))); do
        PORT=${SRC_SHARD_PORTS[$i]}
        ADD_SHARDS_CMD="${ADD_SHARDS_CMD}sh.addShard('rs${i}/src-rs${i}0:${PORT}'); "
    done
    msh "src-mongos:27017" --eval "$ADD_SHARDS_CMD"

    if [[ "${MONGO_VERSION:-8.0}" == 8.* ]]; then
        msh "src-mongos:27017" --eval "db.adminCommand('transitionFromDedicatedConfigServer');"
    fi
fi

if $TARGET; then
    echo "Starting target cluster with $TGT_SHARDS shards (max: $MAX_TGT_SHARDS)"

    TGT_SERVICES="tgt-cfg0"
    for i in $(seq 0 $((TGT_SHARDS - 1))); do
        TGT_SERVICES="$TGT_SERVICES tgt-rs${i}0"
    done

    dcf up -d $TGT_SERVICES

    mwait "tgt-cfg0:28000" && rsinit "tgt/cfg" "tgt-cfg0:28000"

    for i in $(seq 0 $((TGT_SHARDS - 1))); do
        PORT=${TGT_SHARD_PORTS[$i]}
        mwait "tgt-rs${i}0:${PORT}" && rsinit "tgt/rs${i}" "tgt-rs${i}0:${PORT}"
    done

    dcf up -d tgt-mongos && mwait "tgt-mongos:27017"

    ADD_SHARDS_CMD=""
    for i in $(seq 0 $((TGT_SHARDS - 1))); do
        PORT=${TGT_SHARD_PORTS[$i]}
        ADD_SHARDS_CMD="${ADD_SHARDS_CMD}sh.addShard('rs${i}/tgt-rs${i}0:${PORT}'); "
    done
    msh "tgt-mongos:27017" --eval "$ADD_SHARDS_CMD"

    if [[ "${MONGO_VERSION:-8.0}" == 8.* ]]; then
        msh "tgt-mongos:27017" --eval "db.adminCommand('transitionFromDedicatedConfigServer');"
    fi
fi
