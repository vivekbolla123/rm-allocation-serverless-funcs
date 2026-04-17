#!/bin/bash

set -euo pipefail

TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
JAR_SRC=~/rm-allocation-serverless-funcs/rm-allocation-queues/target/rm-file-handler-1.0.jar
JAR_DST=~/rm-file-handler/
SERVICE=rm-file-handler

log() {
    echo "[$TIMESTAMP] [INFO] $*"
}

log_err() {
    echo "[$TIMESTAMP] [ERROR] $*" >&2
}

# Build
log "Starting Maven build..."
if mvn clean package -DskipTests; then
    log "Build successful."
else
    log_err "Maven build failed. Aborting."
    exit 1
fi

# Copy JAR
log "Copying JAR to $JAR_DST..."
if cp "$JAR_SRC" "$JAR_DST"; then
    log "JAR copied successfully."
else
    log_err "Failed to copy JAR. Aborting."
    exit 1
fi

# Restart service
log "Restarting service: $SERVICE..."
if sudo systemctl restart "$SERVICE"; then
    log "Service restart triggered."
else
    log_err "systemctl restart failed."
    exit 1
fi

# Status check
echo ""
echo "==============================="
echo " Service Status: $SERVICE"
echo "==============================="
sudo systemctl status "$SERVICE" --no-pager -l
echo "==============================="