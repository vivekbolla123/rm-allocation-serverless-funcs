#!/bin/bash
set -e

# =============================================================================
# Deploy update_last_sold_fare.py + common_utils.py on EC2
# Run this script directly on the EC2 instance.
# Usage: ./deploy_ec2.sh <env> <branch-or-tag>
# Example: ./deploy_ec2.sh prod main
#          ./deploy_ec2.sh uat feature/xyz
# =============================================================================

ENV="${1}"
BRANCH="${2}"

if [ -z "$ENV" ] || [ -z "$BRANCH" ]; then
    echo "Usage: ./deploy_ec2.sh <env> <branch-or-tag>"
    echo "  env: prod | uat"
    echo "  branch-or-tag: e.g. main, v1.2.0"
    exit 1
fi

# --- Set deploy dir based on env ---
if [ "$ENV" = "prod" ]; then
    DEPLOY_DIR="/home/ubuntu/PROD/rm-scripts"
elif [ "$ENV" = "uat" ]; then
    DEPLOY_DIR="/home/ubuntu/UAT/rm-scripts"
else
    echo "Error: env must be 'prod' or 'uat'. Got: $ENV"
    exit 1
fi

REPO_DIR="/home/ubuntu/rm-allocation-serverless-funcs"

# --- Execution ---
echo ""
echo "###################################################################"
echo "  Deploy: update_last_sold_fare.py"
echo "###################################################################"
echo ""
echo "  Env:        $ENV"
echo "  Branch:     $BRANCH"
echo "  Repo Dir:   $REPO_DIR"
echo "  Deploy Dir: $DEPLOY_DIR"
echo ""

# Pull latest
echo "[1/3] Pulling latest from $BRANCH..."
cd "$REPO_DIR"
git fetch --all
git checkout "$BRANCH"
git pull origin "$BRANCH"

# Copy files to deploy directory
echo "[2/3] Copying files to $DEPLOY_DIR..."
mkdir -p "$DEPLOY_DIR"
cp "$REPO_DIR/QPDynamicAllocation/update_last_sold_fare.py" "$DEPLOY_DIR/"
cp "$REPO_DIR/QPDynamicAllocation/requirements.txt" "$DEPLOY_DIR/"
cp "$REPO_DIR/rm-allocation-configurations/common_utils.py" "$DEPLOY_DIR/"

# Install dependencies
echo "[3/3] Installing dependencies..."
pip install -r "$DEPLOY_DIR/requirements.txt" --quiet

echo ""
echo "###################################################################"
echo "  Deploy complete! [$ENV]"
echo "  Script location: $DEPLOY_DIR/update_last_sold_fare.py"
echo "###################################################################"
