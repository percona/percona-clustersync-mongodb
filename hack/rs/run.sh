#!/usr/bin/env bash

usage() {
    echo "Usage: $0 [--source-only|--target-only]"
}

if (( $# > 1 )); then
    usage >&2
    exit 1
fi

MODE="both"
case "${1:-}" in
    "") ;;
    --source-only) MODE="source" ;;
    --target-only) MODE="target" ;;
    -h | --help)
        usage
        exit 0
        ;;
    *)
        echo "Unknown argument: $1" >&2
        usage >&2
        exit 1
        ;;
esac

BASE=$(dirname "$(dirname "$0")")

# shellcheck source=/dev/null
source "$BASE/util"

RDIR="$BASE/rs"

export compose="$RDIR/compose.yml"

services=()
if [[ "$MODE" != "target" ]]; then
    services+=(rs00 rs01 rs02)
fi
if [[ "$MODE" != "source" ]]; then
    services+=(rs10 rs11 rs12)
fi

dcf up -d "${services[@]}"

if [[ "$MODE" != "target" ]]; then
    mwait "rs00:30000"
    mwait "rs01:30001"
    mwait "rs02:30002"

    rsinit "scripts/rs0" "rs00:30000"
fi

if [[ "$MODE" != "source" ]]; then
    mwait "rs10:30100"
    mwait "rs11:30101"
    mwait "rs12:30102"

    rsinit "scripts/rs1" "rs10:30100"
fi
