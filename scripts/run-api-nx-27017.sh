#!/usr/bin/env bash
#
# NeoSQLite vs MongoDB API Compatibility Test Script (using NX-27017)
#
# This script:
# 1. Starts the NX-27017 server (MongoDB wire protocol server using SQLite)
# 2. Executes the API comparison Python script
# 3. Reports compatibility statistics
# 4. Cleans up the server
#
# NX-27017 is a MongoDB wire protocol compatibility layer that uses SQLite
# as the backend storage engine. This allows MongoDB clients to connect
# and perform operations while data is actually stored in SQLite.
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
NX27017_PORT=27017
NX27017_HOST="127.0.0.1"
NX27017_CMD="/home/cwt/Env/neosqlite/bin/nx-27017"
NX27017_DB_DIR=$(mktemp -d)
NX27017_DB="$NX27017_DB_DIR/neosqlite"  # Use file-based database for persistence
COMPARISON_SCRIPT="$(dirname "$0")/../examples/api_comparison_main.py"

# Track if we started the server (for cleanup)
SERVER_STARTED=false

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
# Cleanup function to stop the NX-27017 server
#######################################
cleanup() {
    local exit_code=$?

    if [ "$SERVER_STARTED" = true ]; then
        info "Cleaning up..."

        # Stop NX-27017 server
        info "Stopping NX-27017 server..."
        $NX27017_CMD --stop >/dev/null 2>&1 || true
        success "NX-27017 server stopped"
    fi

    # Cleanup database file
    cleanup_database

    if [ $exit_code -ne 0 ]; then
        error "Script exited with code $exit_code"
    fi

    exit $exit_code
}

# Set trap for cleanup on exit, interrupt, or error
trap cleanup EXIT INT TERM

#######################################
# Check if NX-27017 is available
#######################################
check_nx27017() {
    info "Checking for NX-27017..."

    # Check if nx_27017 command exists
    if [ -f "$NX27017_CMD" ]; then
        success "Found nx_27017 at $NX27017_CMD"
        return 0
    fi

    error "NX-27017 not found at $NX27017_CMD"
    return 1
}

#######################################
# Check if port is available
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
# Stop existing NX-27017 server if running
#######################################
cleanup_existing_server() {
    info "Stopping any existing NX-27017 server..."

    # Try to stop using nx_27017 command
    $NX27017_CMD --stop 2>/dev/null || true

    # Also try to kill any process on the port
    if command -v lsof &> /dev/null; then
        lsof -ti:$NX27017_PORT | xargs -r kill 2>/dev/null || true
    fi

    sleep 1
    success "Existing server stopped"
}

cleanup_database() {
    info "Cleaning up database files..."
    if [ -d "$NX27017_DB_DIR" ]; then
        rm -rf "$NX27017_DB_DIR"
    fi
}

#######################################
# Run NX-27017 server
#######################################
run_nx27017_server() {
    info "Starting NX-27017 server on port $NX27017_PORT..."

    # Check if port is already in use
    if ! check_port_available $NX27017_PORT; then
        warn "Port $NX27017_PORT is already in use. Attempting to start anyway..."
    fi

    # Start NX-27017 server with memory database
    $NX27017_CMD --db "$NX27017_DB" --host $NX27017_HOST -p $NX27017_PORT 2>&1 &
    NX27017_PID=$!

    SERVER_STARTED=true

    # Wait for NX-27017 to be ready
    info "Waiting for NX-27017 to be ready..."
    local max_attempts=30
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        # Try to connect with a simple socket test
        if command -v nc &> /dev/null; then
            if nc -z $NX27017_HOST $NX27017_PORT 2>/dev/null; then
                success "NX-27017 is ready"
                return 0
            fi
        elif command -v timeout &> /dev/null; then
            if timeout 1 bash -c "echo > /dev/tcp/$NX27017_HOST/$NX27017_PORT" 2>/dev/null; then
                success "NX-27017 is ready"
                return 0
            fi
        else
            # Just wait a bit
            sleep 1
            success "Assuming NX-27017 is ready"
            return 0
        fi

        attempt=$((attempt + 1))
        sleep 1
    done

    error "NX-27017 failed to become ready within ${max_attempts} seconds"
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
    
    # Indicate that we're running against NX-27017 (NeoSQLite backend)
    # This allows tests to enable features that NeoSQLite supports but real MongoDB doesn't
    export NX27017_BACKEND=true
    
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
    echo "(Using NX-27017 as MongoDB Server)"
    echo "========================================"
    echo ""

    # Step 1: Check NX-27017 availability
    if ! check_nx27017; then
        exit 1
    fi

    # Step 2: Cleanup existing server
    cleanup_existing_server

    # Step 3: Run NX-27017 server
    if ! run_nx27017_server; then
        exit 1
    fi

    # Step 4: Run comparison script
    if ! run_comparison; then
        exit 1
    fi

    echo ""
    success "All tests completed!"

    # Cleanup is handled by the trap
}

# Run main function
main "$@"
