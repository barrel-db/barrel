#!/bin/bash
# Setup Python virtualenv for barrel_vectordb
#
# This script creates a project-local virtualenv in .venv/ and installs
# the required dependencies for reranking.
#
# Usage:
#   ./scripts/setup_python_venv.sh              # Create .venv with default requirements
#   ./scripts/setup_python_venv.sh /path/to/venv # Custom venv path
#   ./scripts/setup_python_venv.sh .venv full    # Use full requirements (includes barrel_embed)
#
# Prefers uv for speed, falls back to pip if uv is not installed.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PRIV_DIR="$PROJECT_DIR/priv"

VENV_PATH="${1:-.venv}"
PROFILE="${2:-default}"

cd "$PROJECT_DIR"

# Select requirements file
case "$PROFILE" in
    full)
        REQUIREMENTS="$PRIV_DIR/requirements-full.txt"
        ;;
    *)
        REQUIREMENTS="$PRIV_DIR/requirements.txt"
        ;;
esac

# Check if uv is available
USE_UV=false
if command -v uv &> /dev/null; then
    USE_UV=true
    echo "Using uv for package management (fast mode)"
else
    echo "uv not found, using pip (install uv for faster setup: curl -LsSf https://astral.sh/uv/install.sh | sh)"
fi

# Create venv if it doesn't exist
if [ ! -d "$VENV_PATH" ]; then
    echo "Creating virtualenv in $VENV_PATH..."
    if [ "$USE_UV" = true ]; then
        uv venv "$VENV_PATH"
    else
        python3 -m venv "$VENV_PATH"
    fi
fi

# Install dependencies
echo "Installing dependencies from $REQUIREMENTS..."
if [ "$USE_UV" = true ]; then
    uv pip install -r "$REQUIREMENTS" --python "$VENV_PATH/bin/python"
else
    source "$VENV_PATH/bin/activate"
    pip install --upgrade pip
    pip install -r "$REQUIREMENTS"
fi

VENV_ABS_PATH="$(cd "$VENV_PATH" && pwd)"

echo ""
echo "=========================================="
echo "Setup complete!"
echo "=========================================="
echo ""
echo "Virtualenv location: $VENV_ABS_PATH"
echo "Python executable:   $VENV_ABS_PATH/bin/python"
echo ""
echo "Usage in Erlang:"
echo ""
echo "  {ok, Pid} = barrel_vectordb_rerank:start_link(#{"
echo "      venv => \"$VENV_ABS_PATH\""
echo "  })."
echo ""
echo "To activate manually:"
echo "  source $VENV_PATH/bin/activate"
echo ""
echo "To run integration tests:"
echo "  rebar3 eunit --module=barrel_vectordb_rerank_integration_tests"
