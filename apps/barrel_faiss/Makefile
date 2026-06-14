.PHONY: all compile test clean distclean

all: compile test

compile:
	rebar3 compile

test:
	rebar3 ct

clean:
	rebar3 clean
	rm -rf _build/cmake

distclean: clean
	rm -rf _build
