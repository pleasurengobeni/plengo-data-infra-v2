#!/usr/bin/env bash
# =============================================================================
# deploy.sh — Called by Jenkins after it checks out the repo.
#
# Jenkins already pulled the latest code into $WORKSPACE.
# This script:
#   1. Backs up Web UI generated files from the live server directory
#   2. Copies the freshly pulled code from Jenkins workspace to the server dir
#   3. Restores Web UI generated files (they always win over repo versions)
# =============================================================================

set -euo pipefail

WORKSPACE_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVER_DIR="${DEPLOY_TARGET:-$HOME/datapipeline/airflow}"
BACKUP_DIR="/tmp/plengo_deploy_backup_$$"

echo "▶ Deploy started at $(date)"
echo "▶ Workspace : $WORKSPACE_DIR"
echo "▶ Server dir: $SERVER_DIR"

# ── 1. Backup Web UI generated directories from live server ──────────────────
mkdir -p "$BACKUP_DIR"

for dir in dags/config dags/sql dags/etl; do
  if [ -d "$SERVER_DIR/$dir" ]; then
    cp -r "$SERVER_DIR/$dir" "$BACKUP_DIR/"
    echo "  ✓ Backed up $dir"
  fi
done

# ── 2. Sync workspace (fresh pull) → server directory ────────────────────────
rsync -a --exclude='.git' "$WORKSPACE_DIR/" "$SERVER_DIR/"
echo "  ✓ Synced workspace to $SERVER_DIR"

# ── 3. Restore Web UI generated files (overwrite synced versions) ─────────────
for dir in dags/config dags/sql dags/etl; do
  if [ -d "$BACKUP_DIR/$(basename $dir)" ]; then
    cp -r "$BACKUP_DIR/$(basename $dir)/." "$SERVER_DIR/$dir/"
    echo "  ✓ Restored $dir"
  fi
done

# ── 4. Cleanup ────────────────────────────────────────────────────────────────
rm -rf "$BACKUP_DIR"

echo "✓ Deploy complete at $(date)"
