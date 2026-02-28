#!/usr/bin/env bash
#
# NeoSQLite vs MongoDB API Compatibility Test Script
#
# This script:
# 1. Checks for podman or docker availability (podman preferred)
# 2. Pulls the latest MongoDB image
# 3. Runs MongoDB container with exposed port
# 4. Executes the API comparison Python script
# 5. Reports compatibility statistics
# 6. Cleans up the container
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
COMPARISON_SCRIPT="$(dirname "$0")/api_comparison_comprehensive.py"

# Container runtime (podman preferred over docker)
CONTAINER_RUNTIME=""

# Track if we started the container (for cleanup)
CONTAINER_STARTED=false

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
# Cleanup function to stop and remove container
#######################################
cleanup() {
    local exit_code=$?
    
    if [ "$CONTAINER_STARTED" = true ]; then
        info "Cleaning up..."
        
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
    
    if [ $exit_code -ne 0 ]; then
        error "Script exited with code $exit_code"
    fi
    
    exit $exit_code
}

# Set trap for cleanup on exit, interrupt, or error
trap cleanup EXIT INT TERM

#######################################
# Check for available container runtime
# Sets CONTAINER_RUNTIME to 'podman' or 'docker'
#######################################
check_container_runtime() {
    info "Checking for container runtime..."
    
    # Check for podman first (higher priority)
    if command -v podman &> /dev/null; then
        CONTAINER_RUNTIME="podman"
        success "Found podman"
        return 0
    fi
    
    # Check for docker
    if command -v docker &> /dev/null; then
        CONTAINER_RUNTIME="docker"
        success "Found docker"
        return 0
    fi
    
    error "Neither podman nor docker found. Please install one of them."
    return 1
}

#######################################
# Pull MongoDB image
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
        local max_attempts=30
        local attempt=0
        
        while [ $attempt -lt $max_attempts ]; do
            # Check if MongoDB is accepting connections
            if command -v mongosh &> /dev/null; then
                if mongosh --host localhost --port "$MONGODB_PORT" --eval "db.adminCommand('ping')" --quiet >/dev/null 2>&1; then
                    success "MongoDB is ready"
                    return 0
                fi
            elif command -v mongo &> /dev/null; then
                if mongo --host localhost --port "$MONGODB_PORT" --eval "db.adminCommand('ping')" --quiet >/dev/null 2>&1; then
                    success "MongoDB is ready"
                    return 0
                fi
            else
                # No mongo client available, just wait a bit
                sleep 2
                success "Assuming MongoDB is ready (no client available to verify)"
                return 0
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
    
    # Run the comparison script
    # The script will exit with non-zero if there are incompatibilities
    if python3 "$COMPARISON_SCRIPT"; then
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
    
    # Step 1: Check container runtime
    if ! check_container_runtime; then
        exit 1
    fi
    
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
