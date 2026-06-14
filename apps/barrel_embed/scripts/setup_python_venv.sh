#!/bin/bash
# Setup Python virtualenv for barrel_embed
#
# This script creates a project-local virtualenv in .venv/ and installs
# the required dependencies for embedding providers.
#
# Usage:
#   ./scripts/setup_python_venv.sh [OPTIONS]
#
# Options:
#   --minimal    Install only sentence-transformers (default)
#   --all        Install all providers (torch, transformers, pillow, fastembed)
#   --provider X Install specific provider (sentence-transformers, fastembed, splade, colbert, clip)
#   --dev        Install dev dependencies (test + uvloop)
#   --test       Run Python tests after setup
#   --uvloop     Install uvloop for better async performance
#
# The venv is automatically detected by the embedding providers.

set -e

VENV_DIR=".venv"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
INSTALL_MODE="minimal"
INSTALL_DEV=false
INSTALL_UVLOOP=false
RUN_TESTS=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --all)
            INSTALL_MODE="all"
            shift
            ;;
        --minimal)
            INSTALL_MODE="minimal"
            shift
            ;;
        --provider)
            INSTALL_MODE="provider"
            PROVIDER="$2"
            shift 2
            ;;
        --dev)
            INSTALL_DEV=true
            shift
            ;;
        --test)
            RUN_TESTS=true
            shift
            ;;
        --uvloop)
            INSTALL_UVLOOP=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --minimal    Install only sentence-transformers (default)"
            echo "  --all        Install all providers"
            echo "  --provider X Install specific provider"
            echo "  --dev        Install dev dependencies (test + uvloop)"
            echo "  --uvloop     Install uvloop for better async performance"
            echo "  --test       Run Python tests after setup"
            echo "  -h, --help   Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

cd "$PROJECT_DIR"

# Check if uv is available (preferred for speed)
USE_UV=false
if command -v uv &> /dev/null; then
    USE_UV=true
    echo "Using uv for faster installation..."
fi

# Create venv if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtualenv in $VENV_DIR..."
    if [ "$USE_UV" = true ]; then
        uv venv "$VENV_DIR"
    else
        python3 -m venv "$VENV_DIR"
    fi
fi

# Install dependencies
if [ "$USE_UV" = true ]; then
    # Use uv pip install
    case $INSTALL_MODE in
        minimal)
            echo "Installing minimal dependencies (sentence-transformers)..."
            uv pip install -e "$PROJECT_DIR/priv[sentence-transformers]" --python "$VENV_DIR/bin/python"
            ;;
        all)
            echo "Installing all dependencies..."
            uv pip install -e "$PROJECT_DIR/priv[all]" --python "$VENV_DIR/bin/python"
            ;;
        provider)
            echo "Installing provider: $PROVIDER..."
            uv pip install -e "$PROJECT_DIR/priv[$PROVIDER]" --python "$VENV_DIR/bin/python"
            ;;
    esac

    # Install dev dependencies if requested
    if [ "$INSTALL_DEV" = true ]; then
        echo "Installing dev dependencies (test + uvloop)..."
        uv pip install -e "$PROJECT_DIR/priv[dev]" --python "$VENV_DIR/bin/python"
    elif [ "$INSTALL_UVLOOP" = true ]; then
        echo "Installing uvloop..."
        uv pip install uvloop --python "$VENV_DIR/bin/python" 2>/dev/null || echo "Note: uvloop not available on this platform"
    fi
else
    # Fallback to pip
    source "$VENV_DIR/bin/activate"
    echo "Installing dependencies..."
    pip install --upgrade pip

    case $INSTALL_MODE in
        minimal)
            echo "Installing minimal dependencies (sentence-transformers)..."
            pip install -e "$PROJECT_DIR/priv[sentence-transformers]"
            ;;
        all)
            echo "Installing all dependencies..."
            pip install -e "$PROJECT_DIR/priv[all]"
            ;;
        provider)
            echo "Installing provider: $PROVIDER..."
            pip install -e "$PROJECT_DIR/priv[$PROVIDER]"
            ;;
    esac

    # Install dev dependencies if requested
    if [ "$INSTALL_DEV" = true ]; then
        echo "Installing dev dependencies (test + uvloop)..."
        pip install -e "$PROJECT_DIR/priv[dev]"
    elif [ "$INSTALL_UVLOOP" = true ]; then
        echo "Installing uvloop..."
        pip install -e "$PROJECT_DIR/priv[uvloop]" 2>/dev/null || echo "Note: uvloop not available on this platform"
    fi
fi

VENV_ABS_PATH="$(cd "$VENV_DIR" && pwd)"

echo ""
echo "=========================================="
echo "Setup complete!"
echo "=========================================="
echo ""
echo "Virtualenv location: $VENV_ABS_PATH"
echo "Python executable:   $VENV_ABS_PATH/bin/python"
echo ""
echo "To activate manually:"
echo "  source $VENV_DIR/bin/activate"
echo ""
echo "To use with barrel_embed (recommended):"
echo "  rebar3 shell"
echo "  {ok, S} = barrel_embed:init(#{"
echo "      embedder => {local, #{venv => \"$VENV_ABS_PATH\"}}"
echo "  })."
echo "  barrel_embed:embed(<<\"hello world\">>, S)."
echo ""
echo "To run Python tests:"
echo "  source $VENV_DIR/bin/activate"
echo "  cd priv && pytest tests/ -v"
echo ""

# Run tests if requested
if [ "$RUN_TESTS" = true ]; then
    echo "Running Python tests..."
    echo ""
    cd "$PROJECT_DIR/priv"
    # Install test dependencies if not already installed
    pip install -e ".[test]" -q
    pytest tests/ -v
fi
