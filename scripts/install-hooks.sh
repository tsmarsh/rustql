#!/bin/bash
#
# Install git hooks for rustql
#
# Usage: ./scripts/install-hooks.sh
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the script directory and repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
HOOKS_DIR="$REPO_ROOT/.git/hooks"

echo -e "${YELLOW}Installing git hooks for rustql...${NC}"

# Check if we're in a git repository
if [ ! -d "$REPO_ROOT/.git" ]; then
    echo -e "${RED}Error: Not a git repository${NC}"
    exit 1
fi

# Check if hooks directory exists
if [ ! -d "$HOOKS_DIR" ]; then
    echo -e "${RED}Error: .git/hooks directory not found${NC}"
    exit 1
fi

# Install pre-commit hook
PRE_COMMIT_SRC="$SCRIPT_DIR/pre-commit"
PRE_COMMIT_DST="$HOOKS_DIR/pre-commit"

if [ ! -f "$PRE_COMMIT_SRC" ]; then
    echo -e "${RED}Error: pre-commit hook not found at $PRE_COMMIT_SRC${NC}"
    exit 1
fi

# Back up existing hook if it exists and isn't a symlink to ours
if [ -f "$PRE_COMMIT_DST" ] && [ ! -L "$PRE_COMMIT_DST" ]; then
    BACKUP="$PRE_COMMIT_DST.backup.$(date +%Y%m%d%H%M%S)"
    echo -e "${YELLOW}Backing up existing pre-commit hook to $BACKUP${NC}"
    mv "$PRE_COMMIT_DST" "$BACKUP"
fi

# Create symlink to the hook
echo -e "Installing pre-commit hook..."
ln -sf "$PRE_COMMIT_SRC" "$PRE_COMMIT_DST"
chmod +x "$PRE_COMMIT_SRC"

echo -e "${GREEN}Git hooks installed successfully!${NC}"
echo ""
echo "The following hooks are now active:"
echo "  - pre-commit: Runs cargo fmt, clippy, and test before each commit"
echo ""
echo "To skip hooks temporarily (not recommended), use:"
echo "  git commit --no-verify"
echo ""
echo "To uninstall hooks, run:"
echo "  rm $PRE_COMMIT_DST"
