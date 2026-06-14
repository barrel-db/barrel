#!/bin/bash
# Setup Python venv for barrel_vectordb using uv
#
# Usage:
#   ./scripts/setup_venv.sh                     # Create .venv with default requirements
#   ./scripts/setup_venv.sh /path/to/venv       # Custom venv path
#   ./scripts/setup_venv.sh .venv full          # Use full requirements (includes barrel_embed)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PRIV_DIR="$PROJECT_DIR/priv"

VENV_PATH="${1:-.venv}"
PROFILE="${2:-default}"

# Select requirements file
case "$PROFILE" in
    full)
        REQUIREMENTS="$PRIV_DIR/requirements-full.txt"
        ;;
    *)
        REQUIREMENTS="$PRIV_DIR/requirements.txt"
        ;;
esac

# Check for uv
if ! command -v uv &> /dev/null; then
    echo "Error: uv is not installed. Install with:"
    echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# Create venv if needed
if [ ! -d "$VENV_PATH" ]; then
    echo "Creating venv at $VENV_PATH..."
    uv venv "$VENV_PATH"
fi

# Install packages
echo "Installing packages from $REQUIREMENTS..."
uv pip install -r "$REQUIREMENTS" --python "$VENV_PATH/bin/python"

VENV_ABS_PATH="$(cd "$VENV_PATH" && pwd)"
echo ""
echo "Done! Use with barrel_vectordb_rerank:"
echo ""
echo "  {ok, Pid} = barrel_vectordb_rerank:start_link(#{"
echo "      venv => \"$VENV_ABS_PATH\""
echo "  })."
echo ""
echo "To share with barrel_embed, use the 'full' profile:"
echo "  ./scripts/setup_venv.sh .venv full"
