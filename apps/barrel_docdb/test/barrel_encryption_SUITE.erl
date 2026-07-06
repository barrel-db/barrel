%%%-------------------------------------------------------------------
%%% @doc Encryption at rest for the document + attachment stores:
%%% EncryptedEnv wiring, the CRYPTO key-check marker, and the
%%% fail-closed open matrix (wrong key, plaintext/encrypted
%%% mismatches, provider errors).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_encryption_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    t_encrypted_roundtrip/1,
    t_sentinel_absent_on_disk/1,
    t_attachments_encrypted/1,
    t_wrong_key/1,
    t_plaintext_open_of_encrypted/1,
    t_encrypt_existing_plaintext/1,
    t_key_provider_error/1,
    t_gc_env_retained/1,
    t_branch_encrypted/1,
    t_branch_pitr_encrypted/1,
    t_merge_encrypted/1,
    t_branch_reopen_wrong_key/1,
    t_branch_encryption_mismatch/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(PROVIDER, barrel_docdb_test_keyprovider).
%% high-entropy sentinel: survives snappy block compression literally
-define(SENTINEL, <<"s3ntinel-9f8a7b6c5d4e3f2a1b0c-barrel-enc-d1e2f3a4b5c6">>).

all() ->
    [t_encrypted_roundtrip, t_sentinel_absent_on_disk,
     t_attachments_encrypted, t_wrong_key,
     t_plaintext_open_of_encrypted, t_encrypt_existing_plaintext,
     t_key_provider_error, t_gc_env_retained,
     t_branch_encrypted, t_branch_pitr_encrypted, t_merge_encrypted,
     t_branch_reopen_wrong_key, t_branch_encryption_mismatch].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    DataDir = "/tmp/barrel_test_encryption",
    os:cmd("rm -rf " ++ DataDir),
    [{data_dir, DataDir} | Config].

end_per_suite(Config) ->
    ok = application:stop(barrel_docdb),
    os:cmd("rm -rf " ++ ?config(data_dir, Config)),
    ok.

init_per_testcase(Case, Config) ->
    application:set_env(barrel_docdb, test_encryption_master,
                        <<"suite master secret">>),
    Db = <<"enc_", (atom_to_binary(Case, utf8))/binary>>,
    [{db, Db} | Config].

end_per_testcase(_Case, Config) ->
    application:set_env(barrel_docdb, test_encryption_master,
                        <<"suite master secret">>),
    _ = barrel_docdb:delete_db(<<(?config(db, Config))/binary, "_b">>),
    _ = barrel_docdb:delete_db(?config(db, Config)),
    ok.

enc_opts(Config) ->
    #{data_dir => ?config(data_dir, Config),
      encryption => #{provider => ?PROVIDER}}.

plain_opts(Config) ->
    #{data_dir => ?config(data_dir, Config)}.

db_path(Config) ->
    filename:join(?config(data_dir, Config),
                  binary_to_list(?config(db, Config))).

%% scan every file under Dir for Bin
found_on_disk(Dir, Bin) ->
    filelib:fold_files(Dir, ".*", true,
        fun(_File, true) -> true;
           (File, false) ->
                case file:read_file(File) of
                    {ok, Data} -> binary:match(Data, Bin) =/= nomatch;
                    {error, _} -> false
                end
        end, false).

%%====================================================================
%% Cases
%%====================================================================

t_encrypted_roundtrip(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db, enc_opts(Config)),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>,
                                         <<"v">> => 1}),
    {ok, #{<<"v">> := 1}} = barrel_docdb:get_doc(Db, <<"a">>),
    %% the marker exists and identity is unaffected
    ?assert(filelib:is_regular(filename:join(db_path(Config), "CRYPTO"))),
    {ok, Info} = barrel_docdb:db_info(Db),
    ?assertEqual(Db, maps:get(keyspace, Info)),
    %% reopen with the same key
    ok = barrel_docdb:close_db(Db),
    {ok, _} = barrel_docdb:create_db(Db, enc_opts(Config)),
    {ok, #{<<"v">> := 1}} = barrel_docdb:get_doc(Db, <<"a">>),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"b">>}),
    {ok, _} = barrel_docdb:get_doc(Db, <<"b">>),
    ok.

t_sentinel_absent_on_disk(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db, enc_opts(Config)),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>,
                                         <<"secret">> => ?SENTINEL}),
    ok = barrel_docdb:close_db(Db),
    ?assertNot(found_on_disk(db_path(Config), ?SENTINEL)),
    %% control: the same write in a plaintext db IS on disk in clear,
    %% which proves the scan method sees through WAL/SST encodings
    Plain = <<Db/binary, "_plain">>,
    {ok, _} = barrel_docdb:create_db(Plain, plain_opts(Config)),
    {ok, _} = barrel_docdb:put_doc(Plain, #{<<"id">> => <<"a">>,
                                            <<"secret">> => ?SENTINEL}),
    ok = barrel_docdb:close_db(Plain),
    PlainPath = filename:join(?config(data_dir, Config),
                              binary_to_list(Plain)),
    try
        ?assert(found_on_disk(PlainPath, ?SENTINEL))
    after
        os:cmd("rm -rf " ++ PlainPath)
    end,
    ok.

t_attachments_encrypted(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db, enc_opts(Config)),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>}),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"a">>, <<"f">>,
                                          ?SENTINEL),
    {ok, ?SENTINEL} = barrel_docdb:get_attachment(Db, <<"a">>, <<"f">>),
    ok = barrel_docdb:close_db(Db),
    ?assertNot(found_on_disk(filename:join(db_path(Config),
                                           "attachments"),
                             ?SENTINEL)),
    ok.

t_wrong_key(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db, enc_opts(Config)),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>}),
    ok = barrel_docdb:close_db(Db),
    application:set_env(barrel_docdb, test_encryption_master,
                        <<"another master">>),
    ?assertEqual({error, wrong_encryption_key},
                 barrel_docdb:create_db(Db, enc_opts(Config))),
    %% the right key still opens
    application:set_env(barrel_docdb, test_encryption_master,
                        <<"suite master secret">>),
    {ok, _} = barrel_docdb:create_db(Db, enc_opts(Config)),
    {ok, _} = barrel_docdb:get_doc(Db, <<"a">>),
    ok.

t_plaintext_open_of_encrypted(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db, enc_opts(Config)),
    ok = barrel_docdb:close_db(Db),
    ?assertEqual({error, db_is_encrypted},
                 barrel_docdb:create_db(Db, plain_opts(Config))),
    %% still opens encrypted
    {ok, _} = barrel_docdb:create_db(Db, enc_opts(Config)),
    ok.

t_encrypt_existing_plaintext(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db, plain_opts(Config)),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>}),
    ok = barrel_docdb:close_db(Db),
    ?assertEqual({error, cannot_encrypt_existing_db},
                 barrel_docdb:create_db(Db, enc_opts(Config))),
    %% still opens plaintext
    {ok, _} = barrel_docdb:create_db(Db, plain_opts(Config)),
    {ok, _} = barrel_docdb:get_doc(Db, <<"a">>),
    ok.

t_key_provider_error(Config) ->
    Db = ?config(db, Config),
    application:unset_env(barrel_docdb, test_encryption_master),
    ?assertMatch({error, {encryption_key_error,
                          {key_provider_error, ?PROVIDER, no_test_master}}},
                 barrel_docdb:create_db(Db, enc_opts(Config))),
    %% nothing was created on disk that would block a later open
    {ok, _} = barrel_docdb:create_db(Db, plain_opts(Config)),
    ok.

t_gc_env_retained(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db, enc_opts(Config)),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>}),
    %% a full-node GC must not free the EncryptedEnv the NIF holds
    lists:foreach(fun(P) -> erlang:garbage_collect(P) end, processes()),
    {ok, _} = barrel_docdb:get_doc(Db, <<"a">>),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"b">>}),
    {ok, _} = barrel_docdb:get_doc(Db, <<"b">>),
    ok.

%%====================================================================
%% Timeline on encrypted databases
%%====================================================================

t_branch_encrypted(Config) ->
    Db = ?config(db, Config),
    Branch = <<Db/binary, "_b">>,
    {ok, _} = barrel_docdb:create_db(Db, enc_opts(Config)),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>,
                                         <<"v">> => 1}),
    %% the branch inherits the parent's encryption spec and key-check
    %% marker; it opens under the parent's key and works right away
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    BranchPath = filename:join(?config(data_dir, Config),
                               binary_to_list(Branch)),
    ?assert(filelib:is_regular(filename:join(BranchPath, "CRYPTO"))),
    {ok, #{<<"v">> := 1}} = barrel_docdb:get_doc(Branch, <<"a">>),
    %% branch writes are encrypted too, and stay isolated
    {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"bonly">>,
                                             <<"secret">> => ?SENTINEL}),
    {error, not_found} = barrel_docdb:get_doc(Db, <<"bonly">>),
    ok = barrel_docdb:close_db(Branch),
    ?assertNot(found_on_disk(BranchPath, ?SENTINEL)),
    ok.

t_branch_pitr_encrypted(Config) ->
    Db = ?config(db, Config),
    Branch = <<Db/binary, "_b">>,
    {ok, _} = barrel_docdb:create_db(Db, enc_opts(Config)),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>,
                                         <<"v">> => 1}),
    {ok, _, T} = barrel_docdb:get_changes(Db, first),
    {ok, #{<<"_rev">> := R1}} = barrel_docdb:get_doc(Db, <<"d">>),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>,
                                         <<"v">> => 2,
                                         <<"_rev">> => R1}),
    %% the rewind runs on a direct open of the encrypted checkpoint
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{at => T}),
    {ok, #{<<"v">> := 1}} = barrel_docdb:get_doc(Branch, <<"d">>),
    {ok, #{<<"v">> := 2}} = barrel_docdb:get_doc(Db, <<"d">>),
    ok.

t_merge_encrypted(Config) ->
    Db = ?config(db, Config),
    Branch = <<Db/binary, "_b">>,
    {ok, _} = barrel_docdb:create_db(Db, enc_opts(Config)),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>}),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"feature">>,
                                             <<"done">> => true}),
    {ok, #{docs_written := 1}} = barrel_docdb:merge_branch(Branch, #{}),
    {ok, #{<<"done">> := true}} = barrel_docdb:get_doc(Db, <<"feature">>),
    ok.

t_branch_reopen_wrong_key(Config) ->
    Db = ?config(db, Config),
    Branch = <<Db/binary, "_b">>,
    {ok, _} = barrel_docdb:create_db(Db, enc_opts(Config)),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>}),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    ok = barrel_docdb:close_db(Branch),
    application:set_env(barrel_docdb, test_encryption_master,
                        <<"another master">>),
    ?assertEqual({error, wrong_encryption_key},
                 barrel_docdb:create_db(Branch, enc_opts(Config))),
    application:set_env(barrel_docdb, test_encryption_master,
                        <<"suite master secret">>),
    {ok, _} = barrel_docdb:create_db(Branch, enc_opts(Config)),
    {ok, _} = barrel_docdb:get_doc(Branch, <<"a">>),
    ok.

t_branch_encryption_mismatch(Config) ->
    Db = ?config(db, Config),
    Branch = <<Db/binary, "_b">>,
    BranchPath = filename:join(?config(data_dir, Config),
                               binary_to_list(Branch)),
    %% a plaintext parent cannot fork into an encrypted branch
    {ok, _} = barrel_docdb:create_db(Db, plain_opts(Config)),
    ?assertEqual({error, cannot_encrypt_existing_db},
                 barrel_docdb:branch_db(Db, Branch,
                     #{encryption => #{provider => ?PROVIDER}})),
    ?assertNot(filelib:is_file(BranchPath)),
    ok = barrel_docdb:close_db(Db),
    os:cmd("rm -rf " ++ filename:join(?config(data_dir, Config),
                                      binary_to_list(Db))),
    %% an encrypted parent cannot fork into a plaintext branch
    {ok, _} = barrel_docdb:create_db(Db, enc_opts(Config)),
    ?assertEqual({error, db_is_encrypted},
                 barrel_docdb:branch_db(Db, Branch,
                                        #{encryption => disabled})),
    ?assertNot(filelib:is_file(BranchPath)),
    ok.
