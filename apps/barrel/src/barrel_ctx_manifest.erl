%%%-------------------------------------------------------------------
%%% @doc Snapshot manifests (see docs/architecture/contexts.md): a small root
%%% per generation pointing at content-addressed parts, each part
%%% listing artifacts with their sha256 and size. Pure functions over
%%% a staging directory; no database access.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_manifest).

-export([new_id/1,
         scan/1,
         write/3,
         read/1,
         verify_file/2,
         sha256_file/1]).

-export_type([artifact/0, root/0]).

-define(ROOT, "manifest.json").
-define(PARTS_DIR, "parts").
-define(PART_MAX, 1000).
-define(CHUNK, 1048576).

-type artifact() :: #{path := binary(), sha256 := binary(),
                      bytes := non_neg_integer()}.
-type root() :: map().

%% @doc Mint an id: `Prefix' then 24 lowercase base32 characters
%% (15 random bytes), as context ids `ctx_...'.
-spec new_id(binary()) -> binary().
new_id(Prefix) ->
    <<Prefix/binary, (base32(crypto:strong_rand_bytes(15)))/binary>>.

%% @doc Every regular file under `Dir' (relative paths, sorted) with
%% its sha256 and size. The manifest files themselves are skipped.
-spec scan(file:filename()) -> [artifact()].
scan(Dir) ->
    Files = filelib:fold_files(Dir, ".*", true,
                               fun(F, Acc) -> [F | Acc] end, []),
    Base = filename:split(filename:absname(Dir)),
    Arts = [artifact(Base, F) || F <- Files],
    lists:sort([A || #{path := P} = A <- Arts, not manifest_path(P)]).

artifact(Base, File) ->
    Rel = filename:join(lists:nthtail(length(Base),
                                      filename:split(filename:absname(File)))),
    {ok, Sha} = sha256_file(File),
    #{path => unicode:characters_to_binary(Rel), sha256 => Sha,
      bytes => filelib:file_size(File)}.

manifest_path(<<"manifest.json">>) -> true;
manifest_path(<<"parts/", _/binary>>) -> true;
manifest_path(_) -> false.

%% @doc Write the parts (at most 1000 artifacts each, named by their
%% own sha256) then the root, into `Dir'. `Root0' carries everything
%% but `parts'. Returns the root as written.
-spec write(file:filename(), root(), [artifact()]) ->
    {ok, root()} | {error, term()}.
write(Dir, Root0, Artifacts) ->
    ok = filelib:ensure_path(filename:join(Dir, ?PARTS_DIR)),
    Parts = [write_part(Dir, Chunk) || Chunk <- chunks(Artifacts, ?PART_MAX)],
    Root = Root0#{<<"type">> => <<"snapshot_manifest">>,
                  <<"format">> => 1,
                  <<"parts">> => Parts},
    case write_atomic(filename:join(Dir, ?ROOT), encode(Root)) of
        ok -> {ok, Root};
        {error, _} = Err -> Err
    end.

write_part(Dir, Arts) ->
    Part = #{<<"type">> => <<"manifest_part">>, <<"format">> => 1,
             <<"kind">> => <<"data">>,
             <<"artifacts">> => [art_json(A) || A <- Arts]},
    Bin = encode(Part),
    Sha = hex(crypto:hash(sha256, Bin)),
    Ref = <<?PARTS_DIR, "/sha256-", Sha/binary, ".json">>,
    ok = write_atomic(filename:join(Dir, Ref), Bin),
    #{<<"kind">> => <<"data">>, <<"ref">> => Ref, <<"sha256">> => Sha,
      <<"artifacts">> => length(Arts),
      <<"bytes">> => lists:sum([B || #{bytes := B} <- Arts])}.

art_json(#{path := P, sha256 := S, bytes := B}) ->
    #{<<"path">> => P, <<"sha256">> => S, <<"bytes">> => B}.

%% @doc Read a root and its parts from `Dir', checking each part's
%% sha256. Returns the root and the flat artifact list.
-spec read(file:filename()) ->
    {ok, root(), [artifact()]} | {error, term()}.
read(Dir) ->
    case file:read_file(filename:join(Dir, ?ROOT)) of
        {ok, Bin} ->
            case decode(Bin) of
                {ok, #{<<"type">> := <<"snapshot_manifest">>,
                       <<"format">> := 1, <<"parts">> := Parts} = Root} ->
                    read_parts(Dir, Root, Parts, []);
                {ok, _} ->
                    {error, bad_manifest};
                {error, _} = Err ->
                    Err
            end;
        {error, Reason} ->
            {error, {manifest_unreadable, Reason}}
    end.

read_parts(_Dir, Root, [], Acc) ->
    {ok, Root, lists:append(lists:reverse(Acc))};
read_parts(Dir, Root, [#{<<"ref">> := Ref, <<"sha256">> := Sha} | Rest],
           Acc) ->
    case file:read_file(filename:join(Dir, Ref)) of
        {ok, Bin} ->
            case hex(crypto:hash(sha256, Bin)) of
                Sha ->
                    {ok, #{<<"artifacts">> := Arts}} = decode(Bin),
                    read_parts(Dir, Root, Rest, [[from_json(A) || A <- Arts]
                                                 | Acc]);
                _ ->
                    {error, {part_checksum_mismatch, Ref}}
            end;
        {error, Reason} ->
            {error, {part_unreadable, Ref, Reason}}
    end.

from_json(#{<<"path">> := P, <<"sha256">> := S, <<"bytes">> := B}) ->
    #{path => P, sha256 => S, bytes => B}.

%% @doc Whether `File' has the size and sha256 the artifact records.
-spec verify_file(file:filename(), artifact()) -> boolean().
verify_file(File, #{sha256 := Sha, bytes := Bytes}) ->
    filelib:file_size(File) =:= Bytes andalso
        filelib:is_regular(File) andalso
        sha256_file(File) =:= {ok, Sha}.

%% @doc Streamed sha256 of a file, lowercase hex.
-spec sha256_file(file:filename()) -> {ok, binary()} | {error, term()}.
sha256_file(File) ->
    case file:open(File, [read, raw, binary]) of
        {ok, Fd} ->
            try hash_loop(Fd, crypto:hash_init(sha256))
            after file:close(Fd)
            end;
        {error, _} = Err ->
            Err
    end.

hash_loop(Fd, Ctx) ->
    case file:read(Fd, ?CHUNK) of
        {ok, Data} -> hash_loop(Fd, crypto:hash_update(Ctx, Data));
        eof -> {ok, hex(crypto:hash_final(Ctx))};
        {error, _} = Err -> Err
    end.

%%====================================================================
%% Internal
%%====================================================================

chunks([], _N) -> [[]];
chunks(List, N) when length(List) =< N -> [List];
chunks(List, N) ->
    {Chunk, Rest} = lists:split(N, List),
    [Chunk | chunks(Rest, N)].

encode(Term) ->
    iolist_to_binary(json:encode(Term)).

decode(Bin) ->
    try {ok, json:decode(Bin)}
    catch _:_ -> {error, bad_json}
    end.

write_atomic(File0, Bin) ->
    File = unicode:characters_to_list(File0),
    Tmp = File ++ ".tmp",
    case file:write_file(Tmp, Bin, [raw, sync]) of
        ok -> file:rename(Tmp, File);
        {error, _} = Err -> Err
    end.

hex(Bin) ->
    binary:encode_hex(Bin, lowercase).

base32(Bin) ->
    << <<(b32(C))>> || <<C:5>> <= Bin >>.

b32(C) when C < 26 -> $a + C;
b32(C) -> $2 + C - 26.
