#!/usr/bin/env bash
# Clean up Docker resources for test environments
# Removes: containers, volumes, networks
#
# Usage: ./cleanup.sh [ENV...]
#   (no args)  Clean all (rs, sh, sh-ha)
#   rs         Replica set only
#   sh         Sharded cluster only (3→2 shards, project: s1)
#   sh-ha      Sharded cluster HA only
#
# Example:
#   ./cleanup.sh rs sh    # rs and sh

set -euo pipefail

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
ENVS_TO_CLEAN=()

if [[ $# -eq 0 ]]; then
    ENVS_TO_CLEAN=("rs" "sh" "sh-ha")
else
    while [[ $# -gt 0 ]]; do
        case $1 in
            rs|sh|sh-ha)
                ENVS_TO_CLEAN+=("$1")
                shift
                ;;
            *)
                echo "Unknown environment: $1"
                echo "Valid: rs, sh, sh-ha"
                exit 1
                ;;
        esac
    done
fi

# Function to clean up a compose environment
cleanup_env() {
    local name=$1
    local compose_file=$2
    local project_name=${3:-}

    if [[ ! -f "$compose_file" ]]; then
        echo "Skipping $name (compose file not found)"
        return
    fi

    echo "Cleaning $name..."

    local compose_cmd="docker compose -f $compose_file"
    if [[ -n "$project_name" ]]; then
        compose_cmd="docker compose -p $project_name -f $compose_file"
    fi

    # Stop and remove containers, then remove volumes
    $compose_cmd down --remove-orphans 2>/dev/null || true
    $compose_cmd down -v 2>/dev/null || true
}

# Clean specified environments
for env in "${ENVS_TO_CLEAN[@]}"; do
    case $env in
        rs)
            cleanup_env "rs" "$SCRIPT_DIR/rs/compose.yml"
            docker volume ls -q | grep -E '^rs[0-9]+' | xargs -r docker volume rm 2>/dev/null || true
            ;;
        sh)
            cleanup_env "sh" "$SCRIPT_DIR/sh/compose.yml" "s1"
            docker volume ls -q | grep -E '^(src-|tgt-|s1_)' | xargs -r docker volume rm 2>/dev/null || true
            ;;
        sh-ha)
            cleanup_env "sh-ha" "$SCRIPT_DIR/sh-ha/compose.yml" "s1"
            docker volume ls -q | grep -E '^s1_' | xargs -r docker volume rm 2>/dev/null || true
            ;;
    esac
done

echo "Done."
