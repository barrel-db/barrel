#!/bin/bash
# Setup Python venv for barrel_embed using uv
#
# Usage:
#   ./scripts/setup_venv.sh                          # Create .venv with default requirements
#   ./scripts/setup_venv.sh /path/to/venv            # Create venv at custom path
#   ./scripts/setup_venv.sh .venv requirements.txt   # Use custom requirements file

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PRIV_DIR="$(dirname "$SCRIPT_DIR")/priv"

VENV_PATH="${1:-.venv}"
REQUIREMENTS="${2:-$PRIV_DIR/requirements.txt}"

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "Error: uv is not installed. Install it with:"
    echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"
    echo "  # or"
    echo "  pip install uv"
    exit 1
fi

# Create venv if needed
if [ ! -d "$VENV_PATH" ]; then
    echo "Creating venv at $VENV_PATH..."
    uv venv "$VENV_PATH"
fi

# Install/sync packages
echo "Installing packages from $REQUIREMENTS..."
uv pip install -r "$REQUIREMENTS" --python "$VENV_PATH/bin/python"

echo ""
echo "Done! Use with barrel_embed:"
echo ""
echo "  {ok, S} = barrel_embed:init(#{"
echo "      embedder => {local, #{venv => \"$(realpath "$VENV_PATH")\"}}"
echo "  })."
