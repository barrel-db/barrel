REPO := ../../../..
ZIG ?= zig
EXAMPLE_LIBS := _build/default/lib:$(REPO)/_build/default/lib

.PHONY: prepare run bench
prepare: _build/open_by_customer.wasm
	cd $(REPO) && rebar3 compile
	rebar3 compile
	erlc -Werror -o _build receivables_index.erl receivables.erl

run: prepare
	ERL_LIBS="$(EXAMPLE_LIBS)" erl -noshell -pa _build -s receivables run -s init stop

bench: prepare
	erlc -Werror -o _build receivables_bench.erl
	@echo 'trial,mode,first_load_us,backfill_us,write_p50_us,write_p95_us,write_p99_us,catchup_us,query_p50_us,query_p95_us,query_p99_us,refresh_p95_us,loads,map_calls'
	@set -e; trial=0; for order in 'off native wasm' 'native wasm off' 'wasm off native'; do \
	  trial=$$((trial + 1)); for mode in $$order; do \
	    ERL_LIBS="$(EXAMPLE_LIBS)" erl +S 2:2 -noshell -pa _build \
	      -s receivables_bench run -s init stop -extra "$$trial" "$$mode"; \
	  done; \
	done

_build/open_by_customer.wasm: open_by_customer.zig invoice.zig
	mkdir -p _build
	$(ZIG) build-exe open_by_customer.zig -target wasm32-freestanding -O ReleaseSmall \
	  -fno-entry --export=map --stack 16384 --initial-memory=65536 --max-memory=65536 \
	  --cache-dir _build/zig-cache -femit-bin=$@
