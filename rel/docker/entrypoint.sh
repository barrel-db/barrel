#!/bin/sh
# Starts barrel_server in the container. Without a mounted config, writes
# sys.config from BARREL_* variables. A mounted /etc/barrel/sys.config or
# vm.args is used as is.
set -eu

REL=/opt/barrel_server
CONF=/etc/barrel
RUN=/tmp/barrel
mkdir -p "$RUN"

: "${BARREL_HTTP_PORT:=8080}"
: "${BARREL_DATA_DIR:=/data}"
: "${BARREL_LOG_LEVEL:=info}"
: "${BARREL_NODE_NAME:=barrel@127.0.0.1}"

# Erlang string or binary literal content: escape backslash and double quote.
esc() { printf '%s' "$1" | sed 's/[\\"]/\\&/g'; }

tokens() {
    list="${BARREL_AUTH_TOKENS:-}"
    if [ -n "${BARREL_AUTH_TOKENS_FILE:-}" ]; then
        list="$list,$(tr '\n' ',' < "$BARREL_AUTH_TOKENS_FILE")"
    fi
    out=""
    old_ifs=$IFS; IFS=,
    for t in $list; do
        t=$(printf '%s' "$t" | tr -d ' \r')
        [ -n "$t" ] || continue
        out="${out:+$out, }<<\"$(esc "$t")\">>"
    done
    IFS=$old_ifs
    printf '%s' "$out"
}

write_config() {
    toks=$(tokens)
    if [ -n "$toks" ]; then
        auth="{auth, #{tokens => [$toks]}},"
    else
        auth=""
        echo "barrel: no BARREL_AUTH_TOKENS set, the server accepts every request" >&2
    fi
    data=$(esc "$BARREL_DATA_DIR")
    cat > "$RUN/sys.config" <<EOF
[
 {barrel_server, [
   $auth
   {http_port, $BARREL_HTTP_PORT},
   {data_dir, "$data/docdb"}
 ]},
 {barrel_docdb, [{data_dir, "$data/docdb"}]},
 {barrel_ngram, [{data_dir, "$data/ngram"}]},
 {kernel, [
   {logger_level, $BARREL_LOG_LEVEL},
   {logger, [{handler, default, logger_std_h,
              #{formatter => {logger_formatter, #{single_line => true}}}}]}
 ]}
].
EOF
    echo "$RUN/sys.config"
}

write_vmargs() {
    cookie="${BARREL_COOKIE:-$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')}"
    cat > "$RUN/vm.args" <<EOF
-name $BARREL_NODE_NAME
-setcookie $cookie
+K true
EOF
    echo "$RUN/vm.args"
}

case "${1:-start}" in
    start)
        if [ -f "$CONF/sys.config" ]; then
            RELX_CONFIG_PATH="$CONF/sys.config"
        else
            RELX_CONFIG_PATH=$(write_config)
        fi
        if [ -f "$CONF/vm.args" ]; then
            VMARGS_PATH="$CONF/vm.args"
        else
            VMARGS_PATH=$(write_vmargs)
        fi
        export RELX_CONFIG_PATH VMARGS_PATH
        mkdir -p "$BARREL_DATA_DIR/docdb" "$BARREL_DATA_DIR/ngram"
        exec "$REL/bin/barrel_server" foreground
        ;;
    *)
        # Release commands (remote_console, eval, ping) reuse the running
        # node's vm.args so they find it with the same name and cookie.
        if [ -f "$CONF/vm.args" ]; then VMARGS_PATH="$CONF/vm.args"; else VMARGS_PATH="$RUN/vm.args"; fi
        export VMARGS_PATH
        exec "$REL/bin/barrel_server" "$@"
        ;;
esac
