#!/bin/bash
#
# Run barrel_vectordb benchmarks
#
# Usage:
#   ./scripts/run_benchmarks.sh              # Run all benchmarks
#   ./scripts/run_benchmarks.sh --quick      # Run with fewer iterations
#   ./scripts/run_benchmarks.sh --json       # Export to JSON
#   ./scripts/run_benchmarks.sh --compare baseline.json  # Compare with baseline
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

# Default options
ITERATIONS=100
WARMUP=10
OUTPUT_FORMAT="console"
COMPARE_FILE=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --quick)
            ITERATIONS=10
            WARMUP=3
            shift
            ;;
        --full)
            ITERATIONS=500
            WARMUP=50
            shift
            ;;
        --json)
            OUTPUT_FORMAT="json"
            shift
            ;;
        --csv)
            OUTPUT_FORMAT="csv"
            shift
            ;;
        --all-formats)
            OUTPUT_FORMAT="all"
            shift
            ;;
        --compare)
            COMPARE_FILE="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --quick         Run fewer iterations (10 instead of 100)"
            echo "  --full          Run more iterations (500)"
            echo "  --json          Export results to JSON"
            echo "  --csv           Export results to CSV"
            echo "  --all-formats   Export to both JSON and CSV"
            echo "  --compare FILE  Compare results with baseline JSON file"
            echo "  --help          Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "=========================================="
echo "   barrel_vectordb Benchmark Suite"
echo "=========================================="
echo ""
echo "Configuration:"
echo "  Iterations: $ITERATIONS"
echo "  Warmup:     $WARMUP"
echo "  Output:     $OUTPUT_FORMAT"
echo ""

# Compile with bench profile
echo "Compiling..."
rebar3 as bench compile

# Run benchmarks
echo ""
echo "Running benchmarks..."
echo ""

# Run benchmarks using erl directly with correct paths
erl -pa _build/bench/lib/*/ebin \
    -pa _build/bench/lib/*/bench \
    -pa _build/default/lib/*/ebin \
    -noshell \
    -eval "
application:ensure_all_started(barrel_vectordb),
barrel_vectordb_bench:run_all(#{
    iterations => $ITERATIONS,
    warmup_iterations => $WARMUP,
    output_format => $OUTPUT_FORMAT,
    output_file => \"bench_results\"
}),
halt().
"

# Compare if baseline provided
if [ -n "$COMPARE_FILE" ]; then
    if [ -f "$COMPARE_FILE" ]; then
        echo ""
        echo "Comparing with baseline: $COMPARE_FILE"
        echo ""
        erl -pa _build/bench/lib/*/ebin \
            -pa _build/bench/lib/*/bench \
            -pa _build/default/lib/*/ebin \
            -noshell \
            -eval "
barrel_vectordb_bench_compare:compare(\"$COMPARE_FILE\", \"bench_results.json\"),
halt().
"
    else
        echo "Warning: Baseline file not found: $COMPARE_FILE"
    fi
fi

echo ""
echo "Benchmarks complete!"

if [ "$OUTPUT_FORMAT" = "json" ] || [ "$OUTPUT_FORMAT" = "all" ]; then
    echo "Results saved to: bench_results.json"
fi

if [ "$OUTPUT_FORMAT" = "csv" ] || [ "$OUTPUT_FORMAT" = "all" ]; then
    echo "Results saved to: bench_results.csv"
fi
