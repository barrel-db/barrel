%%%-------------------------------------------------------------------
%%% @doc S3-compatible attachment storage backend.
%%%
%%% Implements `barrel_att_backend' against an S3-compatible object store via
%%% `livery_s3'. Does not implement the attachment feed (`att_changes/4' etc,
%%% see M2) or `checkpoint/2' (branching, see the plan) -- only the required
%%% callbacks (plus `delete/5', nominally optional but called unconditionally
%%% by `barrel_att_store:delete/5', so it must exist).
%%%
%%% == Config ==
%%% `att_opts => #{backend => s3, s3 => #{bucket, endpoint, region,
%%% access_key_id, secret_access_key, ...}}' -- every key in the nested `s3'
%%% map except `bucket' and `part_size' passes straight to `livery_s3:new/1'.
%%%
%%% == Key scheme ==
%%% `DbName/hex(DocId)/AttName'. `DbName' is validated elsewhere to
%%% `[a-z0-9_-]' (safe unescaped); `DocId' is arbitrary bytes with no length
%%% cap, so it is hex-encoded; `AttName' is already validated to exclude
%%% NUL/`/'/`\'. A defensive length check rejects keys that would exceed S3's
%%% 1024-byte cap.
%%%
%%% == Whole-blob vs. streaming ==
%%% The caller already has the full binary in memory for `put/5,6', so it is
%%% always a single `put_object' (S3 single-PUT covers up to 5GB) -- no
%%% multipart there. Streaming (`put_stream'/`write_chunk'/`finish_stream',
%%% the path every *replicated* attachment actually goes through) buffers in
%%% memory and only starts a multipart upload once the buffer crosses
%%% `part_size'; `finish_stream' does a single `put_object' if that never
%%% happens (the common case for small/replicated attachments).
%%%
%%% == Write-conflict detection ==
%%% Opt-in optimistic concurrency via S3 conditional writes: `create_only
%%% => true' (`If-None-Match: "*"') and `expected_etag => Etag' (`If-Match:
%%% Etag') on `put/5,6' and `finish_stream/1'. Default (neither given)
%%% stays unconditional, matching the RocksDB backend. A failed precondition
%%% surfaces as `{error, {conflict, CurrentInfo}}' (what's actually there
%%% now), not just `precondition_failed'.
%%%
%%% Not every S3-compatible store can be trusted to enforce this: MinIO has
%%% since 2023, AWS since 2024, but Garage cannot at all, by its own
%%% documented design (no consensus algorithm) -- and a store that silently
%%% accepts the headers without enforcing them is a worse failure mode than
%%% one that rejects them outright, since a caller would believe it's
%%% protected when it is not. `open/2' runs a capability probe (a
%%% `create_only' put to a throwaway key, then a deliberate second one that
%%% must be rejected) and records the verified result; `create_only'/
%%% `expected_etag' fail fast with `{error, conditional_writes_unsupported}'
%%% if the store didn't verifiably enforce it, rather than silently
%%% proceeding unprotected.
%%%
%%% `create_only'/`expected_etag' apply uniformly whether a write ends up a
%%% single `put_object' or a multipart upload: `livery_s3' >= 0.2.0 supports
%%% conditional writes on `complete_multipart_upload' too (the object is
%%% created there, not at `create_multipart_upload', so completion is the
%%% only place the guard can apply). A losing conditional completion
%%% (`precondition_failed' 412, `conditional_request_conflict' 409 -- the
%%% upload id is dead either way for our purposes, a one-shot `finish_stream'
%%% never retries it -- or `not_found' 404 when an `if_match' completion
%%% lost to a concurrent delete) is reported the same
%%% `{error, {conflict, CurrentInfo}}' shape as the single-put path.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_s3_store).
-behaviour(barrel_att_backend).

-export([open/2, close/1]).
-export([put/5, put/6, get/4, delete/4, delete/5]).
-export([delete_all/3]).
-export([fold/5]).
-export([get_info/4]).
-export([put_stream/5, put_stream/6]).
-export([write_chunk/2, finish_stream/1, abort_stream/1]).
-export([get_stream/4, read_chunk/1, close_stream/1]).

-define(DEFAULT_PART_SIZE, 8 * 1024 * 1024). %% 8 MiB; used starting Step 3
-define(MAX_KEY_SIZE, 1024).
-define(META_DIGEST, <<"digest">>).

-spec open(string(), map()) -> {ok, map()} | {error, term()}.
open(_Path, Options) ->
    S3Opts = maps:get(s3, Options, #{}),
    case maps:find(bucket, S3Opts) of
        error ->
            {error, missing_bucket};
        {ok, Bucket} ->
            _ = application:ensure_all_started(livery_s3),
            ClientOpts = maps:without([bucket, part_size], S3Opts),
            Client = livery_s3:new(ClientOpts),
            PartSize = maps:get(part_size, S3Opts, ?DEFAULT_PART_SIZE),
            ConditionalWrites = probe_conditional_writes(Client, Bucket),
            {ok, #{client => Client, bucket => Bucket, part_size => PartSize,
                   conditional_writes => ConditionalWrites}}
    end.

%% @private A store either verifiably enforces If-None-Match (the second,
%% deliberately-colliding put is rejected) or it's treated as unsupported --
%% including on any probe hiccup unrelated to the property being tested,
%% since this only gates an opt-in safety net, not ordinary operation:
%% failing closed here just means create_only/expected_etag refuse clearly
%% instead of silently proceeding unprotected.
probe_conditional_writes(Client, Bucket) ->
    ProbeKey = <<".barrel_att_s3_probe/",
                (binary:encode_hex(crypto:strong_rand_bytes(8), lowercase))/binary>>,
    Result = case livery_s3:put_object(Client, Bucket, ProbeKey, <<"probe">>,
                                       #{if_none_match => <<"*">>}) of
        {ok, _} ->
            case livery_s3:put_object(Client, Bucket, ProbeKey, <<"probe2">>,
                                      #{if_none_match => <<"*">>}) of
                {error, precondition_failed} -> supported;
                _ -> unsupported
            end;
        _ ->
            unsupported
    end,
    _ = livery_s3:delete_object(Client, Bucket, ProbeKey),
    Result.

-spec close(map()) -> ok.
close(_AttRef) ->
    ok.

-spec put(map(), binary(), binary(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
put(AttRef, DbName, DocId, AttName, Data) ->
    put(AttRef, DbName, DocId, AttName, Data, #{}).

-spec put(map(), binary(), binary(), binary(), binary(), map()) ->
    {ok, map()} | {error, term()}.
put(AttRef, DbName, DocId, AttName, Data, Opts) when is_binary(Data) ->
    with_key(DbName, DocId, AttName, fun(Key) ->
        do_put(AttRef, Key, AttName, Data, Opts)
    end).

do_put(#{client := Client, bucket := Bucket, conditional_writes := CW} = AttRef,
       Key, AttName, Data, Opts) ->
    Digest = compute_digest(Data),
    case check_expected_digest(Digest, Opts) of
        {error, _} = Err ->
            Err;
        ok ->
            case conditional_headers(Opts, CW) of
                {error, _} = Err ->
                    Err;
                {ok, CondHeaders} ->
                    ContentType = maps:get(content_type, Opts, mimerl:filename(AttName)),
                    PutOpts = maps:merge(#{
                        content_type => ContentType,
                        metadata => #{?META_DIGEST => Digest}
                    }, CondHeaders),
                    case livery_s3:put_object(Client, Bucket, Key, Data, PutOpts) of
                        {ok, _} ->
                            {ok, #{name => AttName, content_type => ContentType,
                                   length => byte_size(Data), digest => Digest,
                                   chunked => false}};
                        {error, precondition_failed} ->
                            conflict_error(AttRef, Key, AttName);
                        {error, _} = Err ->
                            Err
                    end
            end
    end.

%% @private origin_hlc (replicated writes) has no feed to check against yet
%% (M2): a write always proceeds. expected_digest is a data-integrity check
%% on the incoming bytes (mirrors barrel_att_store_blob's put/6), distinct
%% from the write-conflict-detection Opts (create_only/expected_etag) below.
check_expected_digest(Digest, Opts) ->
    case maps:get(expected_digest, Opts, undefined) of
        undefined -> ok;
        Digest -> ok;
        _Other -> {error, digest_mismatch}
    end.

%% @private Translates create_only/expected_etag into the S3 headers
%% livery_s3 expects, gated on the store having verifiably enforced them at
%% open/2 -- {ok, #{}} (no headers) when the caller asked for neither, so
%% ordinary unconditional writes never even consult ConditionalWrites.
-spec conditional_headers(map(), supported | unsupported) ->
    {ok, map()} | {error, conditional_writes_unsupported}.
conditional_headers(Opts, ConditionalWrites) ->
    CreateOnly = maps:get(create_only, Opts, false),
    ExpectedEtag = maps:get(expected_etag, Opts, undefined),
    case CreateOnly =:= false andalso ExpectedEtag =:= undefined of
        true ->
            {ok, #{}};
        false ->
            case ConditionalWrites of
                unsupported ->
                    {error, conditional_writes_unsupported};
                supported ->
                    Headers0 = case CreateOnly of
                        true -> #{if_none_match => <<"*">>};
                        false -> #{}
                    end,
                    Headers = case ExpectedEtag of
                        undefined -> Headers0;
                        Etag -> Headers0#{if_match => Etag}
                    end,
                    {ok, Headers}
            end
    end.

%% @private A failed precondition reports what's actually at Key now, so
%% the caller can decide: retry against the new baseline, surface it, or
%% force an overwrite by retrying without the conditional options.
conflict_error(#{client := Client, bucket := Bucket}, Key, AttName) ->
    case livery_s3:head_object(Client, Bucket, Key) of
        {ok, Meta} ->
            Metadata = maps:get(metadata, Meta, #{}),
            {error, {conflict, #{
                name => AttName,
                content_type => maps:get(content_type, Meta,
                                         mimerl:filename(AttName)),
                length => maps:get(content_length, Meta, 0),
                digest => maps:get(?META_DIGEST, Metadata, undefined),
                etag => maps:get(etag, Meta, undefined)
            }}};
        {error, not_found} ->
            %% Raced with a delete between our rejected write and this
            %% HEAD -- report the conflict without current info rather
            %% than pretending the write actually landed.
            {error, {conflict, undefined}};
        {error, _} = Err ->
            Err
    end.

-spec get(map(), binary(), binary(), binary()) ->
    {ok, binary()} | {error, term()}.
get(AttRef, DbName, DocId, AttName) ->
    with_key(DbName, DocId, AttName, fun(Key) ->
        do_get(AttRef, Key)
    end).

do_get(#{client := Client, bucket := Bucket}, Key) ->
    case livery_s3:get_object(Client, Bucket, Key) of
        {ok, #{body := Body}} -> {ok, Body};
        {error, {s3, _Code, _Msg, #{status := 404}}} -> {error, not_found};
        {error, _} = Err -> Err
    end.

-spec delete(map(), binary(), binary(), binary()) -> ok | {error, term()}.
delete(AttRef, DbName, DocId, AttName) ->
    delete(AttRef, DbName, DocId, AttName, #{}).

%% @doc `Opts' (e.g. `origin_hlc') is accepted but unused: there is no feed
%% yet to guard a delete's ordering against (M2). A delete always proceeds,
%% same as an unconditional put.
-spec delete(map(), binary(), binary(), binary(), map()) ->
    ok | {error, term()}.
delete(AttRef, DbName, DocId, AttName, _Opts) ->
    with_key(DbName, DocId, AttName, fun(Key) ->
        do_delete(AttRef, Key)
    end).

do_delete(#{client := Client, bucket := Bucket}, Key) ->
    livery_s3:delete_object(Client, Bucket, Key).

-spec delete_all(map(), binary(), binary()) -> ok | {error, term()}.
delete_all(#{client := Client, bucket := Bucket}, DbName, DocId) ->
    case doc_prefix(DbName, DocId) of
        {error, _} = Err ->
            Err;
        {ok, Prefix} ->
            case livery_s3:list_objects_all(Client, Bucket, #{prefix => Prefix}) of
                {ok, #{objects := []}} ->
                    ok;
                {ok, #{objects := Objects}} ->
                    Keys = [maps:get(key, O) || O <- Objects],
                    case livery_s3:delete_objects(Client, Bucket, Keys) of
                        {ok, _} -> ok;
                        {error, _} = Err -> Err
                    end;
                {error, _} = Err ->
                    Err
            end
    end.

%% @doc Enumerates attachment names under a document. Does not fetch object
%% bodies: the sole caller in this codebase (`barrel_att:list_attachments/3'
%% via `barrel_docdb:list_attachments/2') discards `Data' entirely, and
%% fetching bytes (or even just metadata) per key here would cost N extra S3
%% round trips for a value nothing reads -- `Data' is `undefined'. Revisit if
%% a real consumer of fold's Data argument appears.
-spec fold(map(), binary(), binary(), fun(), term()) -> term().
fold(#{client := Client, bucket := Bucket}, DbName, DocId, Fun, Acc) ->
    case doc_prefix(DbName, DocId) of
        {error, _} ->
            Acc;
        {ok, Prefix} ->
            case livery_s3:list_objects_all(Client, Bucket, #{prefix => Prefix}) of
                {ok, #{objects := Objects}} ->
                    fold_objects(Objects, Prefix, Fun, Acc);
                {error, _} ->
                    Acc
            end
    end.

fold_objects([], _Prefix, _Fun, Acc) ->
    Acc;
fold_objects([#{key := Key} | Rest], Prefix, Fun, Acc) ->
    AttName = att_name_from_key(Prefix, Key),
    case Fun(AttName, undefined, Acc) of
        {ok, Acc1} -> fold_objects(Rest, Prefix, Fun, Acc1);
        {stop, Acc1} -> Acc1;
        stop -> Acc
    end.

-spec get_info(map(), binary(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
get_info(AttRef, DbName, DocId, AttName) ->
    with_key(DbName, DocId, AttName, fun(Key) ->
        do_get_info(AttRef, Key, AttName)
    end).

do_get_info(#{client := Client, bucket := Bucket}, Key, AttName) ->
    case livery_s3:head_object(Client, Bucket, Key) of
        {ok, Meta} ->
            Metadata = maps:get(metadata, Meta, #{}),
            {ok, #{
                name => AttName,
                content_type => maps:get(content_type, Meta,
                                         mimerl:filename(AttName)),
                length => maps:get(content_length, Meta, 0),
                digest => maps:get(?META_DIGEST, Metadata, undefined),
                chunked => false
            }};
        {error, not_found} ->
            {error, not_found};
        {error, _} = Err ->
            Err
    end.

%%====================================================================
%% Key scheme
%%====================================================================

%% @private Resolve the object key, defensively rejecting anything that
%% would exceed S3's 1024-byte key cap, then run Fun against it.
with_key(DbName, DocId, AttName, Fun) ->
    case object_key(DbName, DocId, AttName) of
        {error, _} = Err -> Err;
        {ok, Key} -> Fun(Key)
    end.

-spec object_key(binary(), binary(), binary()) ->
    {ok, binary()} | {error, key_too_long}.
object_key(DbName, DocId, AttName) ->
    case doc_prefix(DbName, DocId) of
        {error, _} = Err -> Err;
        {ok, Prefix} -> bounded_key(<<Prefix/binary, AttName/binary>>)
    end.

-spec doc_prefix(binary(), binary()) -> {ok, binary()} | {error, key_too_long}.
doc_prefix(DbName, DocId) ->
    bounded_key(<<DbName/binary, "/",
                  (binary:encode_hex(DocId, lowercase))/binary, "/">>).

bounded_key(Key) ->
    case byte_size(Key) =< ?MAX_KEY_SIZE of
        true -> {ok, Key};
        false -> {error, key_too_long}
    end.

att_name_from_key(Prefix, Key) ->
    PrefixLen = byte_size(Prefix),
    binary:part(Key, PrefixLen, byte_size(Key) - PrefixLen).

compute_digest(Data) ->
    Digest = crypto:hash(sha256, Data),
    <<"sha256-", (binary:encode_hex(Digest, lowercase))/binary>>.

%%====================================================================
%% Streaming API
%%====================================================================
%%
%% This is the path every *replicated* attachment actually goes through
%% (barrel_rep_att:transfer/6 always calls get_attachment_stream/
%% put_attachment, never the whole-blob API). write_chunk buffers in memory
%% and does NOT start a multipart upload eagerly: a real S3 call only
%% happens once the buffer crosses part_size, and finish_stream does a
%% single put_object if that threshold was never crossed (the common case
%% for small/replicated attachments) instead of a needless 3-call
%% create+upload_part+complete sequence.
%%
%% Stream maps thread through write_chunk/read_chunk exactly like the
%% RocksDB backend's: each call returns the map to pass to the next call,
%% and on {error, _} the caller's last-known-good map (from the previous
%% successful call) is what abort_stream/1 must be given -- it alone
%% accurately reflects what has actually been sent to S3 so far.

-spec put_stream(map(), binary(), binary(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
put_stream(AttRef, DbName, DocId, AttName, ContentType) ->
    put_stream(AttRef, DbName, DocId, AttName, ContentType, #{}).

-spec put_stream(map(), binary(), binary(), binary(), binary(), map()) ->
    {ok, map()} | {error, term()}.
put_stream(AttRef, DbName, DocId, AttName, ContentType, Opts) ->
    with_key(DbName, DocId, AttName, fun(Key) ->
        {ok, #{
            type => write,
            att_ref => AttRef,
            key => Key,
            att_name => AttName,
            content_type => ContentType,
            origin_hlc => maps:get(origin_hlc, Opts, undefined),
            expected_digest => maps:get(expected_digest, Opts, undefined),
            create_only => maps:get(create_only, Opts, false),
            expected_etag => maps:get(expected_etag, Opts, undefined),
            buffer => <<>>,
            digest_ctx => crypto:hash_init(sha256),
            length => 0,
            multipart => undefined
        }}
    end).

-spec write_chunk(map(), binary()) -> {ok, map()} | {error, term()}.
write_chunk(#{type := write, buffer := Buffer, digest_ctx := Ctx,
              length := Length} = Stream, Data) ->
    NewBuffer = <<Buffer/binary, Data/binary>>,
    Stream1 = Stream#{
        buffer => NewBuffer,
        digest_ctx => crypto:hash_update(Ctx, Data),
        length => Length + byte_size(Data)
    },
    case byte_size(NewBuffer) >= part_size(Stream1) of
        true -> flush_part(Stream1, maps:get(multipart, Stream1) =:= undefined);
        false -> {ok, Stream1}
    end.

part_size(#{att_ref := #{part_size := PartSize}}) -> PartSize.

%% @private Upload the buffer as one multipart part, creating the upload
%% first if none exists yet. `WasUndefined' is whether `multipart' was
%% `undefined' before THIS write_chunk call started: if the upload was just
%% created in this same call and the part upload then fails, the caller's
%% retained (pre-call) stream has no upload_id at all to abort_stream later
%% -- self-abort here instead of orphaning it. If the upload already
%% existed from a prior successful call, the caller's own stream already
%% carries it, so abort_stream on that is sufficient; no self-abort here.
%%
%% create_only/expected_etag are NOT applied to create_multipart_upload:
%% the object is created at completion, not here (see finish_stream/1),
%% consistent with where livery_s3 itself applies conditional writes for
%% multipart uploads.
flush_part(#{multipart := undefined} = Stream, WasUndefined) ->
    #{att_ref := #{client := Client, bucket := Bucket}, key := Key,
      content_type := ContentType} = Stream,
    case livery_s3:create_multipart_upload(Client, Bucket, Key,
                                           #{content_type => ContentType}) of
        {ok, UploadId} ->
            Stream1 = Stream#{multipart => #{upload_id => UploadId,
                                             parts => [], part_number => 1}},
            flush_part(Stream1, WasUndefined);
        {error, _} = Err ->
            Err
    end;
flush_part(#{multipart := MP, buffer := Buffer} = Stream, WasUndefined) ->
    #{att_ref := #{client := Client, bucket := Bucket}, key := Key} = Stream,
    #{upload_id := UploadId, parts := Parts, part_number := PartNumber} = MP,
    case livery_s3:upload_part(Client, Bucket, Key, UploadId, PartNumber, Buffer) of
        {ok, #{etag := ETag}} ->
            NewMP = MP#{parts => Parts ++ [{PartNumber, ETag}],
                       part_number => PartNumber + 1},
            {ok, Stream#{multipart => NewMP, buffer => <<>>}};
        {error, Reason} ->
            case WasUndefined of
                true -> _ = livery_s3:abort_multipart_upload(Client, Bucket, Key, UploadId);
                false -> ok
            end,
            {error, Reason}
    end.

%% @doc Finish a write stream: never crossed the multipart threshold ->
%% single put_object with everything buffered (digest checked BEFORE
%% sending anything, since nothing has been written yet); otherwise upload
%% the remainder as the final part (allowed under the size minimum since
%% it's last) and complete -- a digest mismatch or a completion failure
%% aborts the whole multipart upload rather than leaving it dangling.
%% `origin_hlc' is accepted but unused: no feed yet to guard against (M2).
-spec finish_stream(map()) -> {ok, map()} | {error, term()}.
finish_stream(#{type := write, multipart := undefined} = Stream) ->
    #{att_ref := #{client := Client, bucket := Bucket,
                  conditional_writes := CW} = AttRef,
      key := Key, att_name := AttName, content_type := ContentType,
      buffer := Buffer, digest_ctx := Ctx,
      expected_digest := ExpectedDigest} = Stream,
    Digest = finalize_digest(Ctx),
    case digest_ok(Digest, ExpectedDigest) of
        {error, _} = Err ->
            Err;
        ok ->
            case conditional_headers(Stream, CW) of
                {error, _} = Err ->
                    Err;
                {ok, CondHeaders} ->
                    PutOpts = maps:merge(#{content_type => ContentType,
                                           metadata => #{?META_DIGEST => Digest}},
                                        CondHeaders),
                    case livery_s3:put_object(Client, Bucket, Key, Buffer, PutOpts) of
                        {ok, _} ->
                            {ok, #{name => AttName, content_type => ContentType,
                                   length => byte_size(Buffer), digest => Digest,
                                   chunked => false}};
                        {error, precondition_failed} ->
                            conflict_error(AttRef, Key, AttName);
                        {error, _} = Err ->
                            Err
                    end
            end
    end;
finish_stream(#{type := write, multipart := MP} = Stream) ->
    #{att_ref := #{client := Client, bucket := Bucket,
                  conditional_writes := CW} = AttRef,
      key := Key, att_name := AttName, content_type := ContentType,
      buffer := Buffer, length := Length, digest_ctx := Ctx,
      expected_digest := ExpectedDigest} = Stream,
    #{upload_id := UploadId, parts := Parts, part_number := PartNumber} = MP,
    FinalParts = case Buffer of
        <<>> ->
            {ok, Parts};
        _ ->
            case livery_s3:upload_part(Client, Bucket, Key, UploadId,
                                       PartNumber, Buffer) of
                {ok, #{etag := ETag}} -> {ok, Parts ++ [{PartNumber, ETag}]};
                {error, _} = UploadErr -> UploadErr
            end
    end,
    case FinalParts of
        {error, Reason} ->
            _ = livery_s3:abort_multipart_upload(Client, Bucket, Key, UploadId),
            {error, Reason};
        {ok, AllParts} ->
            Digest = finalize_digest(Ctx),
            case digest_ok(Digest, ExpectedDigest) of
                {error, _} = DigestErr ->
                    _ = livery_s3:abort_multipart_upload(Client, Bucket, Key, UploadId),
                    DigestErr;
                ok ->
                    case conditional_headers(Stream, CW) of
                        {error, _} = Err ->
                            _ = livery_s3:abort_multipart_upload(Client, Bucket, Key, UploadId),
                            Err;
                        {ok, CondHeaders} ->
                            complete(AttRef, Key, UploadId, AllParts, CondHeaders,
                                     AttName, ContentType, Length, Digest)
                    end
            end
    end.

%% @private The object is created here, not at create_multipart_upload, so
%% this is where a losing conditional write shows up. precondition_failed
%% (412), conditional_request_conflict (409, a concurrent writer won an
%% if_none_match race), and not_found (404, an if_match completion lost to
%% a concurrent delete) all mean the same thing to our caller regardless of
%% which one the upload_id's exact state maps to -- finish_stream is a
%% one-shot terminal call either way, nothing here retries the same
%% upload_id -- so all three report the same {conflict, CurrentInfo} shape
%% as the single-put path. The abort attempt is harmless best-effort
%% cleanup even when the upload id is already dead (409/404).
complete(#{client := Client, bucket := Bucket} = AttRef, Key, UploadId, AllParts,
         CondHeaders, AttName, ContentType, Length, Digest) ->
    case livery_s3:complete_multipart_upload(Client, Bucket, Key, UploadId,
                                             AllParts, CondHeaders) of
        {ok, _} ->
            {ok, #{name => AttName, content_type => ContentType,
                   length => Length, digest => Digest, chunked => true}};
        {error, Reason} when Reason =:= precondition_failed;
                            Reason =:= conditional_request_conflict;
                            Reason =:= not_found ->
            _ = livery_s3:abort_multipart_upload(Client, Bucket, Key, UploadId),
            conflict_error(AttRef, Key, AttName);
        {error, _} = CompleteErr ->
            _ = livery_s3:abort_multipart_upload(Client, Bucket, Key, UploadId),
            CompleteErr
    end.

finalize_digest(Ctx) ->
    DigestBin = crypto:hash_final(Ctx),
    <<"sha256-", (binary:encode_hex(DigestBin, lowercase))/binary>>.

digest_ok(_Digest, undefined) -> ok;
digest_ok(Digest, Digest) -> ok;
digest_ok(_Digest, _Expected) -> {error, digest_mismatch}.

%% @doc Cleans up only what was actually sent to S3: nothing, if the
%% multipart threshold was never crossed (finish_stream is the only place
%% that would have made anything visible); the in-progress multipart
%% upload otherwise.
-spec abort_stream(map()) -> ok.
abort_stream(#{type := write, multipart := #{upload_id := UploadId}} = Stream) ->
    #{att_ref := #{client := Client, bucket := Bucket}, key := Key} = Stream,
    _ = livery_s3:abort_multipart_upload(Client, Bucket, Key, UploadId),
    ok;
abort_stream(_Stream) ->
    ok.

-spec get_stream(map(), binary(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
get_stream(AttRef, DbName, DocId, AttName) ->
    with_key(DbName, DocId, AttName, fun(Key) ->
        do_get_stream(AttRef, Key, AttName)
    end).

do_get_stream(#{client := Client, bucket := Bucket} = AttRef, Key, AttName) ->
    case livery_s3:get_object(Client, Bucket, Key, #{stream => true}) of
        {ok, #{body := {stream, Reader}}} ->
            {ok, #{type => read, att_ref => AttRef, key => Key,
                   att_name => AttName, reader => Reader}};
        {error, {s3, _Code, _Msg, #{status := 404}}} ->
            {error, not_found};
        {error, _} = Err ->
            Err
    end.

%% @doc Pulls from the `livery_client' reader `get_object' handed back
%% (`stream => true'); `{done, _}' -> `eof' per the behaviour contract, not
%% a 3-tuple, since there is no more data to hand the caller.
-spec read_chunk(map()) -> {ok, binary(), map()} | eof | {error, term()}.
read_chunk(#{type := read, reader := Reader} = Stream) ->
    case livery_client:read(Reader, 30000) of
        {ok, Data, NextReader} -> {ok, Data, Stream#{reader => NextReader}};
        {done, _NextReader} -> eof;
        {error, _} = Err -> Err
    end.

%% @doc No-op, matching the RocksDB backend: an abandoned (not fully
%% drained) read stream's underlying connection is reclaimed by hackney's
%% own pool/timeout machinery, not explicitly cancelled here -- livery_client
%% exposes no cancel for a pull-style reader (only for its separate
%% flow => manual push-stream mechanism, which get_object's `stream => true`
%% option does not use).
-spec close_stream(map()) -> ok.
close_stream(_Stream) ->
    ok.
