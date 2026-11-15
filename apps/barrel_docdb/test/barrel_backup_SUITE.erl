%%% @doc Backups taken while the database stays open, and restores as a
%%% database of its own: documents, history, conflicts, local documents
%%% and attachments read back the same, the copy holds every write
%%% before the backup instant and none after, and a restored database is
%%% a new author under any name.
-module(barrel_backup_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([roundtrip_new_name/1,
         restore_same_name/1,
         backup_under_write_load/1,
         restored_copy_replicates/1,
         encrypted_backup/1,
         none_backend/1,
         unsupported_backend_refused/1,
         existing_dir_refused/1,
         corrupt_file_refused/1,
         taken_name_refused/1,
         not_a_backup/1]).

-define(PROVIDER, barrel_docdb_test_keyprovider).

all() ->
    [roundtrip_new_name, restore_same_name, backup_under_write_load,
     restored_copy_replicates, encrypted_backup, none_backend,
     unsupported_backend_refused, existing_dir_refused, corrupt_file_refused,
     taken_name_refused, not_a_backup].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    application:set_env(barrel_docdb, test_encryption_master, <<"backup master">>),
    Config.

end_per_suite(_Config) ->
    application:unset_env(barrel_docdb, test_encryption_master),
    ok.

init_per_testcase(TC, Config) ->
    Base = filename:join(?config(priv_dir, Config), atom_to_list(TC)),
    DataDir = filename:join(Base, "data"),
    ok = filelib:ensure_path(DataDir),
    Name = atom_to_binary(TC),
    [{data_dir, DataDir}, {backups, filename:join(Base, "backups")},
     {db, Name}, {restored, <<Name/binary, "_r">>}, {peer, <<Name/binary, "_p">>}
     | Config].

end_per_testcase(_TC, Config) ->
    Opts = #{data_dir => ?config(data_dir, Config)},
    [barrel_docdb:delete_db(D, Opts)
     || D <- [?config(db, Config), ?config(restored, Config), ?config(peer, Config)]],
    ok.

%%====================================================================
%% Cases
%%====================================================================

%% Documents, past versions, conflicts, local documents and attachment
%% bytes come back the same under a new name, and the copy is a new
%% author that takes writes.
roundtrip_new_name(Config) ->
    {Db, R, Peer} = names(Config),
    {ok, _} = create(Config, Db),
    {ok, _} = create(Config, Peer),
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 1}),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 2, <<"_rev">> => Rev1}),
    {ok, _} = barrel_rep:replicate(Db, Peer),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"c">>, <<"v">> => 0}),
    {ok, _} = barrel_rep:replicate(Db, Peer),
    {ok, #{<<"_rev">> := C}} = barrel_docdb:get_doc(Db, <<"c">>, #{}),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"c">>, <<"v">> => <<"db">>, <<"_rev">> => C}),
    {ok, _} = barrel_docdb:put_doc(Peer, #{<<"id">> => <<"c">>, <<"v">> => <<"peer">>, <<"_rev">> => C}),
    {ok, _} = barrel_rep:replicate(Peer, Db),
    ok = barrel_docdb:put_local_doc(Db, <<"_local/l">>, #{<<"k">> => 1}),
    Bytes = crypto:strong_rand_bytes(300000),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"a">>, <<"blob">>, Bytes),
    Before = snapshot(Db),
    {ok, #{dir := Dir, backup_hlc := Hlc}} = backup(Config, Db),
    ?assert(is_binary(Hlc)),
    {ok, _} = restore(Config, Dir, R),
    ?assertEqual(Before, snapshot(R)),
    {ok, Bytes} = barrel_docdb:get_attachment(R, <<"a">>, <<"blob">>),
    ?assertNotEqual(barrel_docdb:db_instance_id(Db), barrel_docdb:db_instance_id(R)),
    {ok, _} = barrel_docdb:put_doc(R, #{<<"id">> => <<"new">>}),
    ok.

%% Disaster recovery: the original is gone, the backup comes back under
%% its name, as a new author.
restore_same_name(Config) ->
    {Db, _, _} = names(Config),
    {ok, _} = create(Config, Db),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 1}),
    {ok, OldId} = barrel_docdb:db_instance_id(Db),
    {ok, #{dir := Dir}} = backup(Config, Db),
    ok = barrel_docdb:delete_db(Db, #{data_dir => ?config(data_dir, Config)}),
    {ok, _} = restore(Config, Dir, Db),
    {ok, #{<<"v">> := 1}} = barrel_docdb:get_doc(Db, <<"a">>, #{}),
    {ok, NewId} = barrel_docdb:db_instance_id(Db),
    ?assertNotEqual(OldId, NewId),
    %% the new id is kept by later opens
    ok = barrel_docdb:close_db(Db),
    {ok, _} = create(Config, Db),
    ?assertEqual({ok, NewId}, barrel_docdb:db_instance_id(Db)),
    ok.

%% Writes keep landing while the backup runs: the copy holds every write
%% at or before backup_hlc and none after.
backup_under_write_load(Config) ->
    {Db, R, _} = names(Config),
    {ok, _} = create(Config, Db),
    Parent = self(),
    Writer = spawn_link(fun() -> write_loop(Db, 0, Parent) end),
    timer:sleep(50),
    {ok, #{dir := Dir, backup_hlc := Hlc}} = backup(Config, Db),
    Writer ! stop,
    Written = receive {written, W} -> W end,
    {ok, _} = restore(Config, Dir, R),
    lists:foreach(
      fun({Id, Rev}) ->
              Before = rev_hlc(Rev) =< Hlc,
              case barrel_docdb:get_doc(R, Id, #{}) of
                  {ok, _} -> ?assert(Before);
                  {error, not_found} -> ?assertNot(Before)
              end
      end, Written),
    ?assert(length(Written) > 1),
    ok.

%% A restored copy replicates with the source's peer: concurrent edits
%% from the copy and the peer meet as a conflict on both.
restored_copy_replicates(Config) ->
    {Db, R, Peer} = names(Config),
    {ok, _} = create(Config, Db),
    {ok, _} = create(Config, Peer),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>, <<"v">> => 0}),
    {ok, _} = barrel_rep:replicate(Db, Peer),
    {ok, #{dir := Dir}} = backup(Config, Db),
    {ok, _} = restore(Config, Dir, R),
    {ok, #{<<"_rev">> := Rev}} = barrel_docdb:get_doc(R, <<"d">>, #{}),
    {ok, _} = barrel_docdb:put_doc(R, #{<<"id">> => <<"d">>, <<"v">> => <<"r">>, <<"_rev">> => Rev}),
    {ok, _} = barrel_docdb:put_doc(Peer, #{<<"id">> => <<"d">>, <<"v">> => <<"p">>, <<"_rev">> => Rev}),
    {ok, _} = barrel_rep:replicate(R, Peer),
    {ok, _} = barrel_rep:replicate(Peer, R),
    {ok, A} = barrel_docdb:get_doc(R, <<"d">>, #{conflicts => true}),
    {ok, B} = barrel_docdb:get_doc(Peer, <<"d">>, #{conflicts => true}),
    ?assertEqual(maps:get(<<"_rev">>, A), maps:get(<<"_rev">>, B)),
    ?assertEqual(1, length(maps:get(<<"_conflicts">>, A))),
    ?assertEqual(maps:get(<<"_conflicts">>, A), maps:get(<<"_conflicts">>, B)),
    ok.

%% An encrypted backup stays ciphertext: it restores with the key
%% provider and refuses to open without it.
encrypted_backup(Config) ->
    {Db, R, Peer} = names(Config),
    Enc = #{encryption => #{provider => ?PROVIDER}},
    {ok, _} = create(Config, Db, Enc),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"s">>, <<"secret">> => <<"x">>}),
    {ok, #{dir := Dir, manifest := #{<<"encrypted">> := true}}} = backup(Config, Db),
    ?assertMatch({error, _}, restore(Config, Dir, Peer, #{})),
    {ok, _} = restore(Config, Dir, R, Enc),
    {ok, #{<<"secret">> := <<"x">>}} = barrel_docdb:get_doc(R, <<"s">>, #{}),
    ok.

%% A database without an attachment store backs up and restores.
none_backend(Config) ->
    {Db, R, _} = names(Config),
    Opts = #{att_opts => #{backend => none}},
    {ok, _} = create(Config, Db, Opts),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>}),
    {ok, #{dir := Dir}} = backup(Config, Db),
    {ok, _} = restore(Config, Dir, R, Opts),
    {ok, _} = barrel_docdb:get_doc(R, <<"a">>, #{}),
    ok.

%% A backend whose attachments are not local files (S3) is refused.
unsupported_backend_refused(Config) ->
    {Db, _, _} = names(Config),
    {ok, _} = create(Config, Db,
                     #{att_opts => #{backend => barrel_docdb_test_att_backend_minimal}}),
    ?assertMatch({error, {unsupported_att_backend, _}}, backup(Config, Db)),
    ?assertEqual([], backups_on_disk(Config)),
    ok.

existing_dir_refused(Config) ->
    {Db, _, _} = names(Config),
    {ok, _} = create(Config, Db),
    Dir = filename:join(?config(backups, Config), "taken"),
    ok = filelib:ensure_path(Dir),
    ?assertMatch({error, {backup_dir_exists, _}}, barrel_docdb:backup_db(Db, Dir)),
    ok.

%% A file changed after the backup fails the checksum: nothing restored.
corrupt_file_refused(Config) ->
    {Db, R, _} = names(Config),
    {ok, _} = create(Config, Db),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>}),
    {ok, #{dir := Dir, manifest := #{<<"files">> := [#{<<"path">> := P} | _]}}} =
        backup(Config, Db),
    ok = file:write_file(filename:join(Dir, P), <<"garbage">>, [append]),
    ?assertMatch({error, {corrupt_backup, [P]}}, restore(Config, Dir, R)),
    ?assertNot(barrel_docdb:db_exists(R, #{data_dir => ?config(data_dir, Config)})),
    ok.

taken_name_refused(Config) ->
    {Db, _, _} = names(Config),
    {ok, _} = create(Config, Db),
    {ok, #{dir := Dir}} = backup(Config, Db),
    ?assertEqual({error, {db_exists, Db}}, restore(Config, Dir, Db)),
    ok.

not_a_backup(Config) ->
    ?assertEqual({error, not_a_backup},
                 barrel_docdb:restore_db(?config(priv_dir, Config), <<"x">>)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

names(Config) ->
    {?config(db, Config), ?config(restored, Config), ?config(peer, Config)}.

create(Config, Name) ->
    create(Config, Name, #{}).

create(Config, Name, Opts) ->
    barrel_docdb:create_db(Name, Opts#{data_dir => ?config(data_dir, Config)}).

backup(Config, Db) ->
    Dir = filename:join(?config(backups, Config),
                        binary_to_list(Db) ++ "-" ++ integer_to_list(erlang:unique_integer([positive]))),
    barrel_docdb:backup_db(Db, Dir).

restore(Config, Dir, Name) ->
    restore(Config, Dir, Name, #{}).

restore(Config, Dir, Name, Opts) ->
    barrel_docdb:restore_db(Dir, Name, Opts#{data_dir => ?config(data_dir, Config)}).

backups_on_disk(Config) ->
    case file:list_dir(?config(backups, Config)) of
        {ok, L} -> L;
        {error, enoent} -> []
    end.

%% What a database answers: every document with its conflicts and
%% versions, and its local documents.
snapshot(Db) ->
    {ok, Docs} = barrel_docdb:fold_docs(Db, fun(#{<<"id">> := Id}, Acc) -> {ok, [Id | Acc]} end, []),
    [{Id, barrel_docdb:get_doc(Db, Id, #{conflicts => true}),
      barrel_docdb:get_doc_versions(Db, Id)} || Id <- lists:sort(Docs)]
        ++ [barrel_docdb:get_local_doc(Db, <<"_local/l">>)].

write_loop(Db, N, Parent) ->
    receive
        stop -> Parent ! {written, get(written)}
    after 0 ->
        Id = <<"w", (integer_to_binary(N))/binary>>,
        {ok, #{<<"rev">> := Rev}} = barrel_docdb:put_doc(Db, #{<<"id">> => Id}),
        put(written, [{Id, Rev} | case get(written) of undefined -> []; L -> L end]),
        write_loop(Db, N + 1, Parent)
    end.

%% The hex HLC of a version token, comparable with backup_hlc.
rev_hlc(Rev) ->
    [Hex, _] = binary:split(Rev, <<"@">>),
    string:lowercase(Hex).
