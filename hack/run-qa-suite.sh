#!/usr/bin/env bash
# run-qa-suite.sh — run the full PCSM QA pytest suite locally against the current PCSM worktree.
#
# Mirrors what .github/workflows/ci.yml does, but builds the csync image from the local repo
# (via Dockerfile-csync-local) instead of cloning from upstream main.
#
# Requires:
#   - docker daemon running with default-ulimits.nofile >= 65536 (sharded tests need many fds)
#   - /etc/hosts entries for rs00..rs02, rs10..rs12, src-mongos, tgt-mongos, etc. (see AGENTS.md)
#   - psmdb-testing repo checked out as a sibling of this repo (default: ../psmdb-testing relative to PCSM, override via PSMDB_TESTING_PATH)
#
# Makes ZERO changes to the psmdb-testing repo — uses Dockerfile-csync-local that already lives there.
#
# Usage examples:
#   ./hack/run-qa-suite.sh                                    # full suite, PSMDB 8.0
#   ./hack/run-qa-suite.sh --mongo-version 7.0                # full suite, PSMDB 7.0
#   ./hack/run-qa-suite.sh --rebuild-all                      # force rebuild every image
#   ./hack/run-qa-suite.sh -- test_basic_sync.py -v           # one test file (anything after -- is passed to pytest)
#   ./hack/run-qa-suite.sh --smoke                            # single tiny test as smoke check
#   ./hack/run-qa-suite.sh --shard 0/5                        # only run shard 0 of 5 (matches CI shard 0)
#   ./hack/run-qa-suite.sh --vm                               # run inside Lima VM (workaround for kernel >= 6.19, see SERVER-121912)

set -euo pipefail

PCSM_PATH="${PCSM_PATH:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
PSMDB_TESTING_PATH="${PSMDB_TESTING_PATH:-$(dirname "$PCSM_PATH")/psmdb-testing}"
PYTEST_DIR="$PSMDB_TESTING_PATH/pcsm-pytest"
MONGO_IMAGE="${MONGO_IMAGE:-perconalab/percona-server-mongodb}"
MONGO_VERSION="${MONGO_VERSION:-8.0}"
GO_VERSION="${GO_VERSION:-1.25}"
LIMA_VM_NAME="${LIMA_VM_NAME:-pcsm-qa}"
REBUILD_ALL=0
SMOKE=0
SHARD=""
USE_VM=0
PYTEST_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
    --mongo-version)
        MONGO_VERSION="$2"
        shift 2
        ;;
    --mongo-image)
        MONGO_IMAGE="$2"
        shift 2
        ;;
    --go-version)
        GO_VERSION="$2"
        shift 2
        ;;
    --rebuild-all)
        REBUILD_ALL=1
        shift
        ;;
    --smoke)
        SMOKE=1
        shift
        ;;
    --shard)
        SHARD="$2"
        shift 2
        ;;
    --vm)
        USE_VM=1
        shift
        ;;
    --)
        shift
        PYTEST_ARGS=("$@")
        break
        ;;
    -h | --help)
        sed -n '2,22p' "$0"
        exit 0
        ;;
    *)
        echo "unknown arg: $1" >&2
        exit 2
        ;;
    esac
done

if [[ "$USE_VM" == "1" ]]; then
    command -v limactl >/dev/null || {
        echo "limactl not installed (yay -S lima-bin or brew install lima)" >&2
        exit 1
    }
    limactl shell "$LIMA_VM_NAME" true 2>/dev/null ||
        {
            echo "Lima VM '$LIMA_VM_NAME' not running. Start it: limactl start --tty=false --name=$LIMA_VM_NAME $PCSM_PATH/hack/lima/pcsm-qa.yaml" >&2
            exit 1
        }
    export DOCKER_HOST="unix://$HOME/.lima/$LIMA_VM_NAME/sock/docker.sock"
fi

[[ -d "$PCSM_PATH" ]] || {
    echo "PCSM_PATH not a dir: $PCSM_PATH" >&2
    exit 1
}
[[ -d "$PYTEST_DIR" ]] || {
    echo "pcsm-pytest dir missing: $PYTEST_DIR (set PSMDB_TESTING_PATH)" >&2
    exit 1
}
[[ -f "$PYTEST_DIR/Dockerfile-csync-local" ]] || {
    echo "Dockerfile-csync-local missing in $PYTEST_DIR" >&2
    exit 1
}
docker info >/dev/null 2>&1 || {
    echo "docker daemon not running" >&2
    exit 1
}

echo "==> PCSM repo:      $PCSM_PATH"
echo "==> psmdb-testing:  $PYTEST_DIR"
echo "==> MONGODB_IMAGE:  $MONGO_IMAGE:$MONGO_VERSION"
echo "==> GO_VERSION:     $GO_VERSION"
[[ "$USE_VM" == "1" ]] && echo "==> Lima VM:        $LIMA_VM_NAME (DOCKER_HOST=$DOCKER_HOST)"

cd "$PYTEST_DIR"

echo "==> docker compose down -v --remove-orphans"
docker compose down -v --remove-orphans 2>/dev/null || true

if [[ "$REBUILD_ALL" == "1" ]]; then
    echo "==> --rebuild-all: removing prior local images"
    docker image rm -f csync/local mongodb/local easyrsa/local pcsm-pytest-test 2>/dev/null || true
fi

export MONGODB_IMAGE="$MONGO_IMAGE:$MONGO_VERSION"

echo "==> build easyrsa/local"
docker compose build easyrsa

echo "==> build csync/local from local PCSM worktree"
docker build \
    --build-arg "GO_VERSION=$GO_VERSION" \
    --build-context "repo=$PCSM_PATH" \
    -t csync/local \
    -f Dockerfile-csync-local .

echo "==> build mongodb/local + test (testinfra) images"
docker compose build mongodb test

echo "==> docker compose up -d (test + network)"
docker compose up -d test

PYTEST_BASE=(pytest -s -v -m 'not jenkins' --junitxml=junit.xml)

if [[ -n "$SHARD" ]]; then
    shard_id="${SHARD%/*}"
    num_shards="${SHARD#*/}"
    PYTEST_BASE+=(--shard-id="$shard_id" --num-shards="$num_shards")
fi

if [[ "$SMOKE" == "1" ]]; then
    PYTEST_BASE=(pytest -s -v --timeout=300 'test_basic_sync.py::test_csync_PML_T2[replicaset]')
fi

if ((${#PYTEST_ARGS[@]} > 0)); then
    PYTEST_BASE=(pytest "${PYTEST_ARGS[@]}")
fi

echo "==> running: ${PYTEST_BASE[*]}"
exec docker compose run --rm test "${PYTEST_BASE[@]}"
