# Rebuilds the committed .wasm fixtures. wasm-tools 1.256.0 for WAT,
# Zig 0.17.1 for the receivables guest (sources in the example).
WASM_TOOLS ?= wasm-tools
ZIG ?= zig
EXAMPLE := ../../../../docs/architecture/examples/receivables
WAT := by_customer counter loop fanout

all: $(addsuffix .wasm,$(WAT)) open_by_customer.wasm

%.wasm: %.wat
	$(WASM_TOOLS) parse $< -o $@

open_by_customer.wasm: $(EXAMPLE)/open_by_customer.zig $(EXAMPLE)/invoice.zig
	$(ZIG) build-exe $(EXAMPLE)/open_by_customer.zig -target wasm32-freestanding -O ReleaseSmall \
	  -fno-entry --export=map --stack 16384 --initial-memory=65536 --max-memory=65536 \
	  --cache-dir /tmp/barrel-design-zig-cache -femit-bin=$@
