BASEDIR = $(shell pwd)
SUPPORTDIR = $(BASEDIR)/support
REBAR ?= $(SUPPORTDIR)/rebar3
EPMD ?= $(shell which epmd)

.PHONY: help all rel tar store apply eqc

all: compile

compile:
	@ERLANG_ROCKSDB_OPTS="-DWITH_SNAPPY=ON" $(REBAR) compile

## Create a barrel release
rel:
	@ERLANG_ROCKSDB_OPTS="-DWITH_SNAPPY=ON" $(REBAR) as prod release

devrel: ## Create a barrel release
	@$(REBAR) release

tar: ## Create a tar file containing a portable release
	@$(REBAR) as prod tar

clean:
	@$(REBAR) clean

distclean: clean ## Clean all build and releases artifacts
	@rm -rf _build
	@rm -rf data

cleantest:
	@rm -rf _build/test

dialyzer:
	@$(REBAR) dialyzer

test: cleantest
	@ERLANG_ROCKSDB_OPTS="-DWITH_SNAPPY=ON" $(REBAR) eunit
	@$(EPMD) -daemon
	@ERLANG_ROCKSDB_OPTS="-DWITH_SNAPPY=ON" $(REBAR) ct --sname=barrel_test

cover:
	@$(REBAR) cover
eqc:
	@$(REBAR) as eqc eunit

eqcshell:
	@$(REBAR) as eqc shell --sname barreleqc@localhost

shell:
	@$(REBAR) as eqc shell --sname barrel@localhost


help: ## This documentation
	@echo Build commands for barrel platform:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo Default command is \'compile\'
	@echo Consult README.md for more information.
