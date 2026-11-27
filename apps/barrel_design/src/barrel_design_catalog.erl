%% @doc Installed designs and their module bytes, kept as local documents
%% of the source database: persisted and backed up with it, never
%% replicated, and never part of its changes feed, so no map sees them.
-module(barrel_design_catalog).

-export([designs/1, design/2, put_design/2, delete_design/2,
         module_bytes/2, put_module/3, delete_module/2, modules/1]).

-define(DESIGN, <<"barrel_design/design/">>).
-define(MODULES, <<"barrel_design/module/">>).

-spec designs(binary()) -> {ok, #{binary() => map()}} | {error, term()}.
designs(Db) ->
    case barrel_docdb:fold_local_docs(Db, ?DESIGN,
            fun(_Id, #{<<"design">> := D = #{name := Name}}, Acc) -> Acc#{Name => D};
               (_Id, _Other, Acc) -> Acc
            end, #{}) of
        {ok, Designs} -> {ok, Designs};
        {error, _} = Error -> Error
    end.

-spec design(binary(), binary()) -> {ok, map()} | {error, not_found}.
design(Db, Name) ->
    case barrel_docdb:get_local_doc(Db, <<?DESIGN/binary, Name/binary>>) of
        {ok, #{<<"design">> := D}} -> {ok, D};
        _ -> {error, not_found}
    end.

-spec put_design(binary(), map()) -> ok | {error, term()}.
put_design(Db, #{name := Name} = Design) ->
    barrel_docdb:put_local_doc(Db, <<?DESIGN/binary, Name/binary>>, #{<<"design">> => Design}).

-spec delete_design(binary(), binary()) -> ok | {error, term()}.
delete_design(Db, Name) ->
    case barrel_docdb:delete_local_doc(Db, <<?DESIGN/binary, Name/binary>>) of
        {error, not_found} -> ok;
        Other -> Other
    end.

-spec module_bytes(binary(), binary()) -> {ok, binary()} | {error, not_found}.
module_bytes(Db, Digest) ->
    case barrel_docdb:get_local_doc(Db, module_id(Digest)) of
        {ok, #{<<"bytes">> := Bytes}} -> {ok, Bytes};
        _ -> {error, not_found}
    end.

-spec put_module(binary(), binary(), binary()) -> ok | {error, term()}.
put_module(Db, Digest, Bytes) ->
    barrel_docdb:put_local_doc(Db, module_id(Digest), #{<<"bytes">> => Bytes}).

-spec delete_module(binary(), binary()) -> ok | {error, term()}.
delete_module(Db, Digest) ->
    case barrel_docdb:delete_local_doc(Db, module_id(Digest)) of
        {error, not_found} -> ok;
        Other -> Other
    end.

-spec modules(binary()) -> {ok, [binary()]} | {error, term()}.
modules(Db) ->
    Len = byte_size(?MODULES),
    barrel_docdb:fold_local_docs(Db, ?MODULES,
        fun(<<_:Len/binary, Hex/binary>>, _Doc, Acc) -> [binary:decode_hex(Hex) | Acc] end, []).

module_id(Digest) ->
    <<?MODULES/binary, (binary:encode_hex(Digest, lowercase))/binary>>.
