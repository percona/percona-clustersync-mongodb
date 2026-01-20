#!/usr/bin/env bash
# Recreate sharded clusters (stop, wipe volumes, restart, init)
#
# Usage:
#   ./recreate-cluster.sh --source      # Recreate source cluster (src-*)
#   ./recreate-cluster.sh --target      # Recreate target cluster (tgt-*)
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
            echo "  -s, --source  Recreate source cluster (src-*)"
            echo "  -t, --target  Recreate target cluster (tgt-*)"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if ! $SOURCE && ! $TARGET; then
    echo "Error: Specify --source and/or --target"
    exit 1
fi

if $SOURCE; then
    echo "Recreating sh source cluster..."
    # Detect MongoDB version from running container before stopping
    if [[ -z "${MONGO_VERSION:-}" ]]; then
        MONGO_VERSION=$(get_mongo_version src-rs00)
    fi
    if [[ -n "${MONGO_VERSION:-}" ]]; then
        export MONGO_VERSION
        echo "  Using MongoDB version: $MONGO_VERSION"
    fi
    echo "  Stopping and removing containers..."
    docker compose -f "$SCRIPT_DIR/compose.yml" stop src-mongos src-cfg0 src-rs00 src-rs10 src-rs20 2>/dev/null || true
    docker rm -f src-mongos src-cfg0 src-rs00 src-rs10 src-rs20 2>/dev/null || true
    sleep 1
    echo "  Removing volumes..."
    docker volume rm sh_src-cfg0 sh_src-rs00 sh_src-rs10 sh_src-rs20 2>/dev/null || true
    echo "  Starting containers..."
    docker compose -f "$SCRIPT_DIR/compose.yml" up -d src-cfg0 src-rs00 src-rs10 src-rs20
    echo "  Waiting for MongoDB..."
    sleep 3
    echo "  Initializing config server..."
    docker exec src-cfg0 mongosh --port 27000 --quiet /cfg/src/cfg.js
    echo "  Initializing shards..."
    docker exec src-rs00 mongosh --port 30000 --quiet /cfg/src/rs0.js
    docker exec src-rs10 mongosh --port 30100 --quiet /cfg/src/rs1.js
    docker exec src-rs20 mongosh --port 30200 --quiet /cfg/src/rs2.js
    echo "  Starting mongos..."
    docker compose -f "$SCRIPT_DIR/compose.yml" up -d src-mongos
    sleep 2
    echo "  Adding shards..."
    docker exec src-mongos mongosh --port 27017 --quiet --eval "sh.addShard('rs0/src-rs00:30000')"
    docker exec src-mongos mongosh --port 27017 --quiet --eval "sh.addShard('rs1/src-rs10:30100')"
    docker exec src-mongos mongosh --port 27017 --quiet --eval "sh.addShard('rs2/src-rs20:30200')"
fi

if $TARGET; then
    echo "Recreating sh target cluster..."
    # Detect MongoDB version from running container before stopping
    if [[ -z "${MONGO_VERSION:-}" ]]; then
        MONGO_VERSION=$(get_mongo_version tgt-rs00)
    fi
    if [[ -n "${MONGO_VERSION:-}" ]]; then
        export MONGO_VERSION
        echo "  Using MongoDB version: $MONGO_VERSION"
    fi
    echo "  Stopping and removing containers..."
    docker compose -f "$SCRIPT_DIR/compose.yml" stop tgt-mongos tgt-cfg0 tgt-rs00 tgt-rs10 tgt-rs20 2>/dev/null || true
    docker rm -f tgt-mongos tgt-cfg0 tgt-rs00 tgt-rs10 tgt-rs20 2>/dev/null || true
    sleep 1
    echo "  Removing volumes..."
    docker volume rm sh_tgt-cfg0 sh_tgt-rs00 sh_tgt-rs10 sh_tgt-rs20 2>/dev/null || true
    echo "  Starting containers..."
    docker compose -f "$SCRIPT_DIR/compose.yml" up -d tgt-cfg0 tgt-rs00 tgt-rs10 tgt-rs20
    echo "  Waiting for MongoDB..."
    sleep 3
    echo "  Initializing config server..."
    docker exec tgt-cfg0 mongosh --port 28000 --quiet /cfg/tgt/cfg.js
    echo "  Initializing shards..."
    docker exec tgt-rs00 mongosh --port 40000 --quiet /cfg/tgt/rs0.js
    docker exec tgt-rs10 mongosh --port 40100 --quiet /cfg/tgt/rs1.js
    docker exec tgt-rs20 mongosh --port 40200 --quiet /cfg/tgt/rs2.js
    echo "  Starting mongos..."
    docker compose -f "$SCRIPT_DIR/compose.yml" up -d tgt-mongos
    sleep 2
    echo "  Adding shards..."
    docker exec tgt-mongos mongosh --port 27017 --quiet --eval "sh.addShard('rs0/tgt-rs00:40000')"
    docker exec tgt-mongos mongosh --port 27017 --quiet --eval "sh.addShard('rs1/tgt-rs10:40100')"
    docker exec tgt-mongos mongosh --port 27017 --quiet --eval "sh.addShard('rs2/tgt-rs20:40200')"
fi

echo "Done."
