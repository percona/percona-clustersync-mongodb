#!/usr/bin/env bash
# Start MongoDB replica set clusters for source and/or target
#
# Usage:
#   ./run.sh                # Start both source and target
#   ./run.sh --source       # Start source only (rs0: rs00, rs01, rs02)
#   ./run.sh --target       # Start target only (rs1: rs10, rs11, rs12)
#   ./run.sh -s -t          # Start both (explicit)

set -euo pipefail

BASE=$(dirname "$(dirname "$0")")

# shellcheck source=/dev/null
source "$BASE/util"

RDIR="$BASE/rs"

export compose="$RDIR/compose.yml"

SOURCE=false
TARGET=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--source) SOURCE=true; shift ;;
        -t|--target) TARGET=true; shift ;;
        -h|--help)
            echo "Usage: $0 [--source|-s] [--target|-t]"
            echo "  No flags: start both source and target"
            echo "  -s, --source  Start source cluster only (rs0: rs00, rs01, rs02)"
            echo "  -t, --target  Start target cluster only (rs1: rs10, rs11, rs12)"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Default: start both
if ! $SOURCE && ! $TARGET; then
    SOURCE=true
    TARGET=true
fi

if $SOURCE; then
    dcf up -d rs00 rs01 rs02

    mwait "rs00:50000"
    mwait "rs01:50001"
    mwait "rs02:50002"

    rsinit "scripts/rs0" "rs00:50000"
fi

if $TARGET; then
    dcf up -d rs10 rs11 rs12

    mwait "rs10:50100"
    mwait "rs11:50101"
    mwait "rs12:50102"

    rsinit "scripts/rs1" "rs10:50100"
fi
