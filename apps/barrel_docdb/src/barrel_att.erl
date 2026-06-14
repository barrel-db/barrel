%%%-------------------------------------------------------------------
%%% @doc Attachment API for barrel_docdb
%%%
%%% Provides a higher-level API for attachment operations.
%%% Uses barrel_att_store for storage with BlobDB.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att).

-include("barrel_docdb.hrl").

%% API
-export([
    put_attachment/5,
    get_attachment/4,
    delete_attachment/4,
    delete_doc_attachments/3,
    list_attachments/3,
    attachment_exists/4
]).

%% Utilities
-export([
    make_att_info/3,
    validate_att_name/1
]).

%%====================================================================
%% API
%%====================================================================

%% @doc Store an attachment
-spec put_attachment(barrel_att_store:att_ref(), db_name(), docid(), binary(), binary()) ->
    {ok, att_info()} | {error, term()}.
put_attachment(AttRef, DbName, DocId, AttName, Data) ->
    case validate_att_name(AttName) of
        ok ->
            barrel_att_store:put(AttRef, DbName, DocId, AttName, Data);
        {error, _} = Error ->
            Error
    end.

%% @doc Retrieve an attachment
-spec get_attachment(barrel_att_store:att_ref(), db_name(), docid(), binary()) ->
    {ok, binary()} | {error, not_found} | {error, term()}.
get_attachment(AttRef, DbName, DocId, AttName) ->
    case barrel_att_store:get(AttRef, DbName, DocId, AttName) of
        {ok, Data} -> {ok, Data};
        not_found -> {error, not_found};
        {error, _} = Error -> Error
    end.

%% @doc Delete an attachment
-spec delete_attachment(barrel_att_store:att_ref(), db_name(), docid(), binary()) ->
    ok | {error, term()}.
delete_attachment(AttRef, DbName, DocId, AttName) ->
    barrel_att_store:delete(AttRef, DbName, DocId, AttName).

%% @doc Delete all attachments for a document
-spec delete_doc_attachments(barrel_att_store:att_ref(), db_name(), docid()) ->
    ok | {error, term()}.
delete_doc_attachments(AttRef, DbName, DocId) ->
    barrel_att_store:delete_all(AttRef, DbName, DocId).

%% @doc List all attachment names for a document
-spec list_attachments(barrel_att_store:att_ref(), db_name(), docid()) -> [binary()].
list_attachments(AttRef, DbName, DocId) ->
    barrel_att_store:fold(AttRef, DbName, DocId,
        fun(Name, _Data, Acc) -> {ok, [Name | Acc]} end,
        []).

%% @doc Check if an attachment exists
-spec attachment_exists(barrel_att_store:att_ref(), db_name(), docid(), binary()) -> boolean().
attachment_exists(AttRef, DbName, DocId, AttName) ->
    case barrel_att_store:get(AttRef, DbName, DocId, AttName) of
        {ok, _} -> true;
        not_found -> false;
        {error, _} -> false
    end.

%%====================================================================
%% Utilities
%%====================================================================

%% @doc Create attachment info from data
-spec make_att_info(binary(), binary(), binary()) -> att_info().
make_att_info(AttName, Data, ContentType) ->
    Digest = compute_digest(Data),
    #{
        name => AttName,
        content_type => ContentType,
        length => byte_size(Data),
        digest => Digest
    }.

%% @doc Validate attachment name
-spec validate_att_name(binary()) -> ok | {error, invalid_att_name}.
validate_att_name(<<>>) ->
    {error, invalid_att_name};
validate_att_name(Name) when is_binary(Name) ->
    %% Check for invalid characters (control chars, path separators)
    case binary:match(Name, [<<0>>, <<"/">>, <<"\\">>]) of
        nomatch -> ok;
        _ -> {error, invalid_att_name}
    end;
validate_att_name(_) ->
    {error, invalid_att_name}.

%%====================================================================
%% Internal Functions
%%====================================================================

%% Compute SHA-256 digest of data
compute_digest(Data) ->
    Digest = crypto:hash(sha256, Data),
    <<"sha256-", (to_hex(Digest))/binary>>.

%% Convert binary to hex string
to_hex(Bin) ->
    << <<(hex_char(N))>> || <<N:4>> <= Bin >>.

hex_char(N) when N < 10 -> $0 + N;
hex_char(N) -> $a + N - 10.
