%% @doc barrel_docdb-backed document backend for barrel_vectordb.
%%
%% Stores each vector's text and metadata as a document in a `barrel_docdb'
%% database, so documents and vectors can share one source of truth (and, later,
%% replicate together). Vectors stay in the vector store's own RocksDB; only text
%% and metadata are kept here.
%%
%% Metadata is stored as an opaque `term_to_binary' value so arbitrary Erlang
%% terms (including atom keys) round-trip exactly through the document codec.
%%
%% Config: `#{db => binary()}' (the docdb database name, default = the store
%% name) and `#{docdb_opts => map()}' (passed to `barrel_docdb:create_db/2').
%% @end
-module(barrel_vectordb_docdb_backend).
-behaviour(barrel_vectordb_docstore).

-export([init/2, put/4, multi_put/2, get/2, multi_get/2, delete/2, terminate/1]).

%% @private
init(Name, Config) ->
    DbName = maps:get(db, Config, atom_to_binary(Name, utf8)),
    DocOpts = maps:get(docdb_opts, Config, #{}),
    case ensure_db(DbName, DocOpts) of
        {ok, _Pid} -> {ok, #{db => DbName}};
        {error, _} = Err -> Err
    end.

%% @private
put(#{db := Db}, Id, Text, Metadata) ->
    Doc = #{
        <<"id">> => Id,
        <<"text">> => Text,
        <<"meta">> => term_to_binary(Metadata)
    },
    case barrel_docdb:put_doc(Db, Doc) of
        {ok, _} -> ok;
        {error, _} = Err -> Err
    end.

%% @private
multi_put(Ctx, Pairs) ->
    lists:foldl(
        fun
            ({Id, Text, Metadata}, ok) -> put(Ctx, Id, Text, Metadata);
            (_Pair, {error, _} = Err) -> Err
        end,
        ok,
        Pairs
    ).

%% @private
get(#{db := Db}, Id) ->
    case barrel_docdb:get_doc(Db, Id) of
        {ok, Doc} ->
            Text = maps:get(<<"text">>, Doc, <<>>),
            Metadata = decode_meta(maps:get(<<"meta">>, Doc, undefined)),
            {ok, Text, Metadata};
        {error, not_found} -> not_found;
        {error, _} = Err -> Err
    end.

%% @private
multi_get(Ctx, Ids) ->
    [get(Ctx, Id) || Id <- Ids].

%% @private
delete(#{db := Db}, Id) ->
    case barrel_docdb:delete_doc(Db, Id) of
        {ok, _} -> ok;
        {error, not_found} -> ok;
        {error, _} = Err -> Err
    end.

%% @private
terminate(#{db := Db}) ->
    _ = barrel_docdb:close_db(Db),
    ok.

%%====================================================================
%% Internal
%%====================================================================

ensure_db(DbName, DocOpts) ->
    case barrel_docdb:open_db(DbName) of
        {ok, Pid} -> {ok, Pid};
        {error, _} -> barrel_docdb:create_db(DbName, DocOpts)
    end.

decode_meta(undefined) -> #{};
decode_meta(Bin) when is_binary(Bin) ->
    try binary_to_term(Bin)
    catch _:_ -> #{}
    end;
decode_meta(_) -> #{}.
