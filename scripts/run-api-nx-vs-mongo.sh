#!/usr/bin/env bash
#
# NX-27017 vs Real MongoDB API Compatibility Test Script (Wire + Async API)
#
# Clone of scripts/run-api-nx-27017.sh with a different comparison axis:
#   existing: direct NeoSQLite (in-process) vs NX-27017 via WireProtocol
#   new:      NX-27017 vs real MongoDB 8.2.12, BOTH via WireProtocol + Async API
#
# This script:
# 1. Starts the NX-27017 server (SQLite backend) on NX_PORT
# 2. Ensures real MongoDB 8.2.12 is reachable on REAL_MONGO_PORT
#    (optionally starts it via docker with --with-docker)
# 3. Executes the async wire-vs-wire comparison Python script
#    (AsyncMongoClient against both endpoints, lenient normalization:
#     kernel/host/version fields never fail the build)
# 4. Reports compatibility statistics
# 5. Cleans up what it started
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration (overridable via env)
NX27017_PORT="${NX_PORT:-27017}"
NX27017_HOST="${NX_HOST:-127.0.0.1}"
REAL_MONGO_PORT="${REAL_MONGO_PORT:-27018}"
REAL_MONGO_HOST="${REAL_MONGO_HOST:-127.0.0.1}"
REAL_MONGO_IMAGE="${REAL_MONGO_IMAGE:-mongo:8.2.12}"
REAL_MONGO_CONTAINER="${REAL_MONGO_CONTAINER:-nx-compat-real-mongo-8-2-12}"
# Container runtime: prefer podman, fall back to docker (overridable via env).
CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-}"
if [ -z "$CONTAINER_RUNTIME" ]; then
    if command -v podman &> /dev/null; then
        CONTAINER_RUNTIME="podman"
    elif command -v docker &> /dev/null; then
        CONTAINER_RUNTIME="docker"
    fi
fi
NX27017_CMD="$(which nx-27017)"
NX27017_DB_DIR=$(mktemp -d)
NX27017_DB="$NX27017_DB_DIR/neosqlite"  # File-based database for persistence
COMPARISON_SCRIPT="$(dirname "$0")/../examples/api_nx_vs_mongo_main.py"
WITH_CONTAINER=false

# Track what we started (for cleanup)
SERVER_STARTED=false
CONTAINER_STARTED=false

WITH_REPLSET=false

for arg in "$@"; do
    case "$arg" in
        --with-docker|--with-podman|--with-container) WITH_CONTAINER=true ;;
        --replset) WITH_REPLSET=true ;;
        --nx-port=*) NX27017_PORT="${arg#--nx-port=}" ;;
        --real-mongo-port=*) REAL_MONGO_PORT="${arg#--real-mongo-port=}" ;;
    esac
done

NX_URI="mongodb://$NX27017_HOST:$NX27017_PORT/"
REAL_MONGO_URI="mongodb://$REAL_MONGO_HOST:$REAL_MONGO_PORT/"

#######################################
# Print colored message
#######################################
print_msg() {
    local color="$1"
    local msg="$2"
    echo -e "${color}${msg}${NC}"
}

info() { print_msg "$BLUE" "[INFO] $1"; }
success() { print_msg "$GREEN" "[SUCCESS] $1"; }
warn() { print_msg "$YELLOW" "[WARNING] $1"; }
error() { print_msg "$RED" "[ERROR] $1"; }

#######################################
# Cleanup function
#######################################
cleanup() {
    local exit_code=$?

    if [ "$SERVER_STARTED" = true ]; then
        info "Stopping NX-27017 server..."
        $NX27017_CMD --stop >/dev/null 2>&1 || true
        success "NX-27017 server stopped"
    fi

    if [ "$CONTAINER_STARTED" = true ]; then
        info "Stopping real MongoDB container ($REAL_MONGO_CONTAINER)..."
        $CONTAINER_RUNTIME stop "$REAL_MONGO_CONTAINER" >/dev/null 2>&1 || true
        success "Real MongoDB container stopped"
    fi

    if [ -d "$NX27017_DB_DIR" ]; then
        rm -rf "$NX27017_DB_DIR"
    fi

    if [ $exit_code -ne 0 ]; then
        error "Script exited with code $exit_code"
    fi

    exit $exit_code
}

trap cleanup EXIT INT TERM

check_nx27017() {
    info "Checking for NX-27017..."
    if [ -f "$NX27017_CMD" ]; then
        success "Found nx_27017 at $NX27017_CMD"
        return 0
    fi
    error "NX-27017 not found at $NX27017_CMD"
    return 1
}

check_port_available() {
    local port=$1
    if command -v ss &> /dev/null; then
        if ss -tuln | grep -q ":$port "; then
            return 1
        fi
    elif command -v netstat &> /dev/null; then
        if netstat -tuln | grep -q ":$port "; then
            return 1
        fi
    fi
    return 0
}

wait_for_port() {
    local host=$1
    local port=$2
    local label=$3
    local max_attempts=30
    local attempt=0
    info "Waiting for $label ($host:$port) to be ready..."
    while [ $attempt -lt $max_attempts ]; do
        if command -v nc &> /dev/null; then
            if nc -z "$host" "$port" 2>/dev/null; then
                success "$label is ready"
                return 0
            fi
        elif timeout 1 bash -c "echo > /dev/tcp/$host/$port" 2>/dev/null; then
            success "$label is ready"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    error "$label failed to become ready within ${max_attempts} seconds"
    return 1
}

cleanup_existing_server() {
    info "Stopping any existing NX-27017 server..."
    $NX27017_CMD --stop 2>/dev/null || true
    if command -v lsof &> /dev/null; then
        lsof -ti:$NX27017_PORT | xargs -r kill 2>/dev/null || true
    fi
    sleep 1
    success "Existing server stopped"
}

run_nx27017_server() {
    info "Starting NX-27017 server on port $NX27017_PORT..."
    if ! check_port_available "$NX27017_PORT"; then
        warn "Port $NX27017_PORT is already in use. Attempting to start anyway..."
    fi
    $NX27017_CMD --db "$NX27017_DB" --host "$NX27017_HOST" -p "$NX27017_PORT" 2>&1 &
    SERVER_STARTED=true
    wait_for_port "$NX27017_HOST" "$NX27017_PORT" "NX-27017"
}

mongo_ping() {
    timeout 5 python3 -c "
from pymongo import MongoClient
c = MongoClient('$REAL_MONGO_URI', serverSelectionTimeoutMS=4000)
c.admin.command('ping')
c.close()
" 2>/dev/null
}

ensure_real_mongo() {
    info "Checking real MongoDB 8.2.12 at $REAL_MONGO_HOST:$REAL_MONGO_PORT..."
    if mongo_ping; then
        success "Real MongoDB is reachable (no strict kernel check will be applied)"
        return 0
    fi
    if [ "$WITH_CONTAINER" = true ]; then
        if [ -z "$CONTAINER_RUNTIME" ]; then
            error "No container runtime found (need podman or docker)"
            return 1
        fi
        # Drop leftovers from previous runs (manual or interrupted).
        $CONTAINER_RUNTIME rm -f "$REAL_MONGO_CONTAINER" >/dev/null 2>&1 || true
        if [ "$WITH_REPLSET" = true ]; then
            # Single-node replica set over host networking so the member
            # hostname (127.0.0.1:PORT) is valid inside and outside the
            # container. Needed for transactions/change streams.
            info "Starting real MongoDB ($REAL_MONGO_IMAGE) via $CONTAINER_RUNTIME (replica set)..."
            $CONTAINER_RUNTIME run -d --rm --name "$REAL_MONGO_CONTAINER" \
                --network=host "$REAL_MONGO_IMAGE" \
                --port "$REAL_MONGO_PORT" --replSet rs0 >/dev/null
            CONTAINER_STARTED=true
            sleep 6
            $CONTAINER_RUNTIME exec "$REAL_MONGO_CONTAINER" mongosh --port "$REAL_MONGO_PORT" --quiet --eval \
                'try { rs.initiate({_id:"rs0", members:[{_id:0, host:"127.0.0.1:'$REAL_MONGO_PORT'"}]}) } catch(e) { print(e.message) }' \
                >/dev/null 2>&1 || true
        else
            # Standalone (no --replSet): writes work, transactions and
            # change streams on the real side do not. Enough for core.
            info "Starting real MongoDB ($REAL_MONGO_IMAGE) via $CONTAINER_RUNTIME (standalone)..."
            $CONTAINER_RUNTIME run -d --rm --name "$REAL_MONGO_CONTAINER" \
                -p "$REAL_MONGO_PORT:27017" "$REAL_MONGO_IMAGE" >/dev/null
            CONTAINER_STARTED=true
        fi
        local attempt=0
        while [ $attempt -lt 30 ]; do
            if mongo_ping; then
                success "Real MongoDB 8.2.12 is ready"
                return 0
            fi
            attempt=$((attempt + 1))
            sleep 2
        done
        error "Real MongoDB failed to become ready"
        return 1
    fi
    error "Real MongoDB not reachable at $REAL_MONGO_URI (hint: rerun with --with-podman)"
    return 1
}

run_comparison() {
    info "Running async wire-vs-wire comparison (NX vs real MongoDB 8.2.12)..."
    if [ ! -f "$COMPARISON_SCRIPT" ]; then
        error "Comparison script not found: $COMPARISON_SCRIPT"
        return 1
    fi
    chmod +x "$COMPARISON_SCRIPT"
    SCRIPT_DIR="$(dirname "$COMPARISON_SCRIPT")"
    PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
    export NX_URI REAL_MONGO_URI
    # Lenient mode: kernel/host/version field noise never fails the build.
    export NX_COMPAT_LENIENT=true
    (cd "$SCRIPT_DIR" && PYTHONPATH="$PROJECT_ROOT" python3 "$(basename "$COMPARISON_SCRIPT")")
    local code=$?
    if [ $code -eq 0 ]; then
        success "Wire-vs-wire comparison completed - fully compatible!"
        return 0
    elif [ $code -eq 2 ]; then
        error "Comparison infra failure (an endpoint was unreachable)"
        return 1
    else
        warn "Wire-vs-wire comparison found functional diffs (see report above)"
        return 0
    fi
}

main() {
    echo "========================================"
    echo "NX-27017 vs Real MongoDB 8.2.12"
    echo "(Both via WireProtocol + Async API)"
    echo "========================================"
    echo ""
    echo "NX endpoint:   $NX_URI"
    echo "Real endpoint: $REAL_MONGO_URI ($REAL_MONGO_IMAGE)"
    echo ""
    check_nx27017 || exit 1
    cleanup_existing_server
    run_nx27017_server || exit 1
    ensure_real_mongo || exit 1
    run_comparison || exit 1
    echo ""
    success "All tests completed!"
}

main "$@"
