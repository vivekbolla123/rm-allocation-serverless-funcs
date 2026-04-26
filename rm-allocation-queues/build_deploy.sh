#!/bin/bash

set -eu

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

# Execution section
clear
echo ""
echo "###################################################################"
echo "[[[  Running: git fetch --all on current working directory: ]]]"
echo "$PWD"
git fetch --all
echo "###################################################################"  && sleep 2
echo ""
echo ""
if [ -z "${1:-}" ]; then
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    echo "###################################################################"
    echo "[[[  No branch provided as parameter.  ]]]"
    echo "[[[  Current branch is: $CURRENT_BRANCH  ]]]"
    echo "###################################################################" && sleep 2
    echo ""
    
    read -p "Do you want to proceed with current branch '$CURRENT_BRANCH'? (yes/no) " Proceed_Current
    case $Proceed_Current in
        yes ) echo "Proceeding with current branch...";;
        no ) echo "Exiting..."; exit 0;;
        * ) echo "Invalid input"; exit 1;;
    esac
    echo ""
    echo ""
else
    echo "###################################################################"
    echo "[[[  Running git checkout on $1  ]]]"
    git checkout "$1"
    echo "###################################################################" && sleep 2
    echo ""
    echo ""
fi
echo "###################################################################"
echo "[[[  Git status O/P: ]]]"
Git_Status=`git status|head -n 5`
echo "$Git_Status"
echo "###################################################################" && sleep 2
echo ""
echo ""

read -p "Do you want to proceed? (yes/no) " User_Input
case $User_Input in
        yes ) echo Proceeding with deployment;
              ;;
        no ) echo Exiting...;
              exit;;
        * ) echo Invalid input; exit;;
esac

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