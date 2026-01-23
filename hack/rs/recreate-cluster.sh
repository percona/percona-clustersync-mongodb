#!/usr/bin/env bash
# Recreate replica set clusters (stop, wipe volumes, restart, init)
#
# Usage:
#   ./recreate-cluster.sh --source      # Recreate source cluster (rs00, rs01, rs02)
#   ./recreate-cluster.sh --target      # Recreate target cluster (rs10, rs11, rs12)
#   ./recreate-cluster.sh -s -t         # Recreate both

set -euo pipefail
SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")

# Get MongoDB version from container image, prints warning if not found
get_mongo_version() {
    local container=$1
    local version
    version=$(docker inspect "$container" --format '{{.Config.Image}}' 2>/dev/null | sed 's/.*://')
    if [[ -z "$version" ]]; then
        echo "  Warning: Could not detect MongoDB version from $container. Set MONGO_VERSION env var if needed." >&2
        return
    fi
    echo "$version"
}

SOURCE=false
TARGET=false

# Parse flags
while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--source) SOURCE=true; shift ;;
        -t|--target) TARGET=true; shift ;;
        -h|--help)
            echo "Usage: $0 [--source|-s] [--target|-t]"
            echo "  -s, --source  Recreate source cluster (rs00, rs01, rs02)"
            echo "  -t, --target  Recreate target cluster (rs10, rs11, rs12)"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if ! $SOURCE && ! $TARGET; then
    echo "Error: Specify --source and/or --target"
    exit 1
fi

if $SOURCE; then
    echo "Recreating rs source cluster..."
    # Detect MongoDB version from running container before stopping
    if [[ -z "${MONGO_VERSION:-}" ]]; then
        MONGO_VERSION=$(get_mongo_version rs00)
    fi
    if [[ -n "${MONGO_VERSION:-}" ]]; then
        export MONGO_VERSION
        echo "  Using MongoDB version: $MONGO_VERSION"
    fi
    echo "  Stopping and removing containers..."
    docker compose -f "$SCRIPT_DIR/compose.yml" stop rs00 rs01 rs02 2>/dev/null || true
    docker rm -f rs00 rs01 rs02 2>/dev/null || true
    sleep 1
    echo "  Removing volumes..."
    docker volume rm rs_rs00 rs_rs01 rs_rs02 2>/dev/null || true
    echo "  Starting containers..."
    docker compose -f "$SCRIPT_DIR/compose.yml" up -d rs00 rs01 rs02
    echo "  Waiting for MongoDB..."
    sleep 3
    echo "  Initializing replica set..."
    docker exec rs00 mongosh --port 30000 --quiet /cfg/scripts/rs0.js
fi

if $TARGET; then
    echo "Recreating rs target cluster..."
    # Detect MongoDB version from running container before stopping
    if [[ -z "${MONGO_VERSION:-}" ]]; then
        MONGO_VERSION=$(get_mongo_version rs10)
    fi
    if [[ -n "${MONGO_VERSION:-}" ]]; then
        export MONGO_VERSION
        echo "  Using MongoDB version: $MONGO_VERSION"
    fi
    echo "  Stopping and removing containers..."
    docker compose -f "$SCRIPT_DIR/compose.yml" stop rs10 rs11 rs12 2>/dev/null || true
    docker rm -f rs10 rs11 rs12 2>/dev/null || true
    sleep 1
    echo "  Removing volumes..."
    docker volume rm rs_rs10 rs_rs11 rs_rs12 2>/dev/null || true
    echo "  Starting containers..."
    docker compose -f "$SCRIPT_DIR/compose.yml" up -d rs10 rs11 rs12
    echo "  Waiting for MongoDB..."
    sleep 3
    echo "  Initializing replica set..."
    docker exec rs10 mongosh --port 30100 --quiet /cfg/scripts/rs1.js
fi

echo "Done."
