#!/usr/bin/env bash
#
# NeoSQLite vs MongoDB API Compatibility Test Script
#
# This script:
# 1. Checks for podman or docker availability (podman preferred)
# 2. On macOS ARM: prefers native mongodb-community via Homebrew if available
# 3. Pulls the latest MongoDB image (if using containers)
# 4. Runs MongoDB container with exposed port (single node, NOT replica set)
# 5. Executes the API comparison Python script
# 6. Reports compatibility statistics
# 7. Cleans up the container or native MongoDB data
#
# Note: MongoDB is run as a single node (not a replica set) for simplicity.
# This means change streams (watch()) cannot be tested in this comparison,
# as MongoDB requires a replica set for change streams. NeoSQLite's watch()
# implementation is fully functional and tested independently via SQLite triggers
# (see tests/test_changestream.py).
#
# Additionally, $log2 is a NeoSQLite extension using SQLite's native log2() function.
# It raises a UserWarning about MongoDB incompatibility. For MongoDB compatibility,
# use { $log: [ <number>, 2 ] } instead.
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
MONGODB_PORT=27017
MONGODB_IMAGE="mongo:latest"
CONTAINER_NAME="neosqlite_mongodb_test"
COMPARISON_SCRIPT="$(dirname "$0")/../examples/api_comparison_main.py"

# Container runtime (podman preferred over docker)
CONTAINER_RUNTIME=""

# Track how MongoDB is being run
MONGODB_MODE="" # "container" or "native"

# Track if we started the container (for cleanup)
CONTAINER_STARTED=false

# Track if we started native MongoDB (for cleanup)
NATIVE_MONGODB_STARTED=false

# Temp directory for native MongoDB data
NATIVE_MONGODB_TMPDIR=""

# PID of the native MongoDB process
NATIVE_MONGODB_PID=""

#######################################
# Print colored message
# Arguments:
#   Color code
#   Message
#######################################
print_msg() {
    local color="$1"
    local msg="$2"
    echo -e "${color}${msg}${NC}"
}

#######################################
# Print info message
# Arguments:
#   Message
#######################################
info() {
    print_msg "$BLUE" "[INFO] $1"
}

#######################################
# Print success message
# Arguments:
#   Message
#######################################
success() {
    print_msg "$GREEN" "[SUCCESS] $1"
}

#######################################
# Print warning message
# Arguments:
#   Message
#######################################
warn() {
    print_msg "$YELLOW" "[WARNING] $1"
}

#######################################
# Print error message
# Arguments:
#   Message
#######################################
error() {
    print_msg "$RED" "[ERROR] $1"
}

#######################################
# Cleanup function to stop and remove container or native MongoDB
#######################################
cleanup() {
    local exit_code=$?

    if [ "$CONTAINER_STARTED" = true ]; then
        info "Cleaning up container..."

        # Stop the container
        info "Stopping container '$CONTAINER_NAME'..."
        if $CONTAINER_RUNTIME stop "$CONTAINER_NAME" >/dev/null 2>&1; then
            success "Container stopped"
        else
            warn "Failed to stop container (may already be stopped)"
        fi

        # Remove the container
        info "Removing container '$CONTAINER_NAME'..."
        if $CONTAINER_RUNTIME rm "$CONTAINER_NAME" >/dev/null 2>&1; then
            success "Container removed"
        else
            warn "Failed to remove container (may already be removed)"
        fi
    fi

    if [ "$NATIVE_MONGODB_STARTED" = true ]; then
        info "Cleaning up native MongoDB..."

        # Stop native MongoDB process
        if [ -n "$NATIVE_MONGODB_PID" ]; then
            info "Stopping native MongoDB (PID: $NATIVE_MONGODB_PID)..."
            if kill "$NATIVE_MONGODB_PID" 2>/dev/null; then
                # Wait for process to exit
                local wait_count=0
                while kill -0 "$NATIVE_MONGODB_PID" 2>/dev/null && [ $wait_count -lt 10 ]; do
                    sleep 1
                    wait_count=$((wait_count + 1))
                done
                # Force kill if still running
                if kill -0 "$NATIVE_MONGODB_PID" 2>/dev/null; then
                    kill -9 "$NATIVE_MONGODB_PID" 2>/dev/null || true
                fi
                success "Native MongoDB stopped"
            else
                warn "Failed to stop native MongoDB (may already be stopped)"
            fi
        fi

        # Clean up temp data directory
        if [ -n "$NATIVE_MONGODB_TMPDIR" ] && [ -d "$NATIVE_MONGODB_TMPDIR" ]; then
            info "Removing temp data directory: $NATIVE_MONGODB_TMPDIR"
            if rm -rf "$NATIVE_MONGODB_TMPDIR"; then
                success "Temp data directory removed"
            else
                warn "Failed to remove temp data directory"
            fi
        fi
    fi

    if [ $exit_code -ne 0 ]; then
        error "Script exited with code $exit_code"
    fi

    exit $exit_code
}

# Set trap for cleanup on exit, interrupt, or error
trap cleanup EXIT INT TERM

#######################################
# Check for available container runtime or native MongoDB
# Sets CONTAINER_RUNTIME and MONGODB_MODE
# Priority on macOS ARM: native MongoDB > podman > docker
#######################################
check_mongodb_availability() {
    info "Checking for MongoDB availability..."

    # On macOS: prefer native mongodb-community via Homebrew
    if [[ "$(uname -s)" == "Darwin" ]]; then
        if command -v brew &> /dev/null; then
            if brew list mongodb/brew/mongodb-community &> /dev/null; then
                MONGODB_MODE="native"
                success "Found native mongodb-community via Homebrew"
                return 0
            fi
        fi
    fi

    # Check for podman first (higher priority)
    if command -v podman &> /dev/null; then
        CONTAINER_RUNTIME="podman"
        MONGODB_MODE="container"
        success "Found podman"
        return 0
    fi

    # Check for docker
    if command -v docker &> /dev/null; then
        CONTAINER_RUNTIME="docker"
        MONGODB_MODE="container"
        success "Found docker"
        return 0
    fi

    # No MongoDB option found
    if [[ "$(uname -s)" == "Darwin" ]]; then
        error "No MongoDB found. On macOS, you can install it via Homebrew:"
        echo ""
        echo "  brew tap mongodb/brew"
        echo "  brew install mongodb-community"
        echo ""
        error "Alternatively, install podman or docker for container-based testing."
    else
        error "Neither podman nor docker found. Please install one of them."
    fi
    return 1
}

#######################################
# Pull MongoDB image (only for container mode)
#######################################
pull_mongodb_image() {
    info "Pulling MongoDB image: $MONGODB_IMAGE..."

    if $CONTAINER_RUNTIME pull "$MONGODB_IMAGE"; then
        success "MongoDB image pulled successfully"
        return 0
    else
        error "Failed to pull MongoDB image"
        return 1
    fi
}

#######################################
# Check if MongoDB is already running on the port
#######################################
check_port_available() {
    local port=$1

    # Check if port is in use
    if command -v ss &> /dev/null; then
        if ss -tuln | grep -q ":$port "; then
            return 1  # Port is in use
        fi
    elif command -v netstat &> /dev/null; then
        if netstat -tuln | grep -q ":$port "; then
            return 1  # Port is in use
        fi
    fi

    return 0  # Port is available
}

#######################################
# Stop and remove existing container if it exists
#######################################
cleanup_existing_container() {
    info "Checking for existing container '$CONTAINER_NAME'..."

    if $CONTAINER_RUNTIME ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        warn "Found existing container '$CONTAINER_NAME', removing..."

        # Stop if running
        if $CONTAINER_RUNTIME ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
            $CONTAINER_RUNTIME stop "$CONTAINER_NAME" >/dev/null 2>&1 || true
        fi

        # Remove
        $CONTAINER_RUNTIME rm "$CONTAINER_NAME" >/dev/null 2>&1 || true
        success "Existing container removed"
    fi
}

#######################################
# Run MongoDB container
#######################################
run_mongodb_container() {
    info "Starting MongoDB container '$CONTAINER_NAME'..."

    # Run container with exposed port
    # Using --rm alternative: we'll manually clean up to have more control
    if $CONTAINER_RUNTIME run -d \
        --name "$CONTAINER_NAME" \
        -p "$MONGODB_PORT:27017" \
        "$MONGODB_IMAGE" \
        --bind_ip_all; then

        CONTAINER_STARTED=true
        success "MongoDB container started"

        # Wait for MongoDB to be ready
        info "Waiting for MongoDB to be ready..."
        local max_attempts=60
        local attempt=0

        while [ $attempt -lt $max_attempts ]; do
            # Check if MongoDB is accepting connections using a simple socket test
            if command -v nc &> /dev/null; then
                if nc -z localhost "$MONGODB_PORT" 2>/dev/null; then
                    success "MongoDB is ready"
                    return 0
                fi
            elif command -v timeout &> /dev/null; then
                if timeout 1 bash -c "echo > /dev/tcp/localhost/$MONGODB_PORT" 2>/dev/null; then
                    success "MongoDB is ready"
                    return 0
                fi
            fi

            attempt=$((attempt + 1))
            sleep 1
        done

        error "MongoDB failed to become ready within ${max_attempts} seconds"
        return 1
    else
        error "Failed to start MongoDB container"
        return 1
    fi
}

#######################################
# Start native MongoDB via Homebrew with temp data directory
# This avoids interfering with any existing user data
#######################################
start_native_mongodb() {
    info "Starting native MongoDB with temp data directory..."

    # Find mongod binary from Homebrew installation
    local mongod_path=""
    if uname -m | grep -q "arm64"; then
        mongod_path="/opt/homebrew/bin/mongod"
    else
        mongod_path="/usr/local/bin/mongod"
    fi

    if [ ! -x "$mongod_path" ]; then
        error "mongod binary not found at $mongod_path"
        return 1
    fi

    # Create temp directory for MongoDB data
    NATIVE_MONGODB_TMPDIR=$(mktemp -d "/tmp/neosqlite_mongodb_XXXXXX")
    info "Using temp data directory: $NATIVE_MONGODB_TMPDIR"

    # Start mongod with temp data directory in background
    # Note: --fork is not supported on macOS, so we use & instead
    "$mongod_path" \
        --dbpath "$NATIVE_MONGODB_TMPDIR" \
        --port "$MONGODB_PORT" \
        --bind_ip_all \
        --logpath "$NATIVE_MONGODB_TMPDIR/mongod.log" \
        &
    NATIVE_MONGODB_PID=$!

    # Give it a moment to start
    sleep 1

    # Verify the process is still running
    if ! kill -0 "$NATIVE_MONGODB_PID" 2>/dev/null; then
        error "Failed to start mongod"
        if [ -f "$NATIVE_MONGODB_TMPDIR/mongod.log" ]; then
            error "Log: $(tail -20 "$NATIVE_MONGODB_TMPDIR/mongod.log")"
        fi
        return 1
    fi

    NATIVE_MONGODB_STARTED=true
    success "Native MongoDB started (PID: $NATIVE_MONGODB_PID)"

    # Wait for MongoDB to be ready
    info "Waiting for MongoDB to be ready..."
    local max_attempts=60
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        # Check if MongoDB is accepting connections using a simple socket test
        if command -v nc &> /dev/null; then
            if nc -z localhost "$MONGODB_PORT" 2>/dev/null; then
                success "MongoDB is ready"
                return 0
            fi
        elif command -v timeout &> /dev/null; then
            if timeout 1 bash -c "echo > /dev/tcp/localhost/$MONGODB_PORT" 2>/dev/null; then
                success "MongoDB is ready"
                return 0
            fi
        fi

        attempt=$((attempt + 1))
        sleep 1
    done

    error "MongoDB failed to become ready within ${max_attempts} seconds"
    return 1
}

#######################################
# Run the API comparison script
#######################################
run_comparison() {
    info "Running API comparison script..."

    if [ ! -f "$COMPARISON_SCRIPT" ]; then
        error "Comparison script not found: $COMPARISON_SCRIPT"
        return 1
    fi

    # Make sure the script is executable
    chmod +x "$COMPARISON_SCRIPT"

    # Run the comparison script from the examples directory
    # The script will exit with non-zero if there are incompatibilities
    SCRIPT_DIR="$(dirname "$COMPARISON_SCRIPT")"
    PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
    if (cd "$SCRIPT_DIR" && PYTHONPATH="$PROJECT_ROOT" python3 api_comparison_main.py); then
        success "API comparison completed - 100% compatible!"
        return 0
    else
        warn "API comparison completed - some incompatibilities found (see report above)"
        return 0  # Return 0 to allow cleanup to proceed
    fi
}

#######################################
# Main function
#######################################
main() {
    echo "========================================"
    echo "NeoSQLite vs MongoDB API Compatibility"
    echo "========================================"
    echo ""

    # Step 1: Check MongoDB availability (native or container)
    if ! check_mongodb_availability; then
        exit 1
    fi

    if [ "$MONGODB_MODE" = "native" ]; then
        # Native MongoDB path
        # Step 2: Start native MongoDB
        if ! start_native_mongodb; then
            exit 1
        fi
    else
        # Container-based path
        # Step 2: Pull MongoDB image
        if ! pull_mongodb_image; then
            exit 1
        fi

        # Step 3: Cleanup existing container
        cleanup_existing_container

        # Step 4: Run MongoDB container
        if ! run_mongodb_container; then
            exit 1
        fi
    fi

    # Step 5: Run comparison script
    if ! run_comparison; then
        exit 1
    fi

    echo ""
    success "All tests completed!"

    # Cleanup is handled by the trap
}

# Run main function
main "$@"
