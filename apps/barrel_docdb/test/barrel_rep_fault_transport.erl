%% Test transport: the local transport with injected failures, set per
%% call in persistent_term `barrel_rep_faults' (#{{Fun, Key} => Error}).
-module(barrel_rep_fault_transport).

-export([get_doc/3, put_version/5, diff_versions/2, get_changes/3,
         get_local_doc/2, put_local_doc/3, delete_local_doc/2, db_info/1,
         sync_hlc/2]).
-export([set/1, clear/0]).

-define(L, barrel_rep_transport_local).

set(Faults) -> persistent_term:put(barrel_rep_faults, Faults).
clear() -> persistent_term:erase(barrel_rep_faults), ok.

fault(Key) -> maps:get(Key, persistent_term:get(barrel_rep_faults, #{}), none).

get_doc(Db, DocId, Opts) ->
    case fault({get_doc, DocId}) of
        none -> ?L:get_doc(Db, DocId, Opts);
        Err -> Err
    end.

put_version(Db, #{<<"id">> := DocId} = Doc, Token, VV, Deleted) ->
    case fault({put_version, DocId}) of
        none -> ?L:put_version(Db, Doc, Token, VV, Deleted);
        Err -> Err
    end.

diff_versions(Db, TokenMap) -> ?L:diff_versions(Db, TokenMap).
get_changes(Db, Since, Opts) -> ?L:get_changes(Db, Since, Opts).

get_local_doc(Db, DocId) ->
    case fault({get_local_doc, Db}) of
        none -> ?L:get_local_doc(Db, DocId);
        Err -> Err
    end.

put_local_doc(Db, DocId, Doc) ->
    case fault({put_local_doc, Db}) of
        none -> ?L:put_local_doc(Db, DocId, Doc);
        Err -> Err
    end.

delete_local_doc(Db, DocId) -> ?L:delete_local_doc(Db, DocId).
db_info(Db) -> ?L:db_info(Db).
sync_hlc(Db, Hlc) -> ?L:sync_hlc(Db, Hlc).
