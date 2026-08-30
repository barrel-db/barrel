%%%-------------------------------------------------------------------
%%% @doc A replication transport whose peer refuses every request with
%%% `unauthorized' (what the HTTP transport returns on 401). Used to
%%% prove checkpoint failures come back as errors, not crashes.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep_unauth_transport).

-export([get_doc/3, put_version/5, diff_versions/2, get_changes/3,
         get_local_doc/2, put_local_doc/3, delete_local_doc/2,
         db_info/1, sync_hlc/2]).

get_doc(_, _, _) -> {error, unauthorized}.
put_version(_, _, _, _, _) -> {error, unauthorized}.
diff_versions(_, _) -> {error, unauthorized}.
get_changes(_, _, _) -> {error, unauthorized}.
get_local_doc(_, _) -> {error, unauthorized}.
put_local_doc(_, _, _) -> {error, unauthorized}.
delete_local_doc(_, _) -> {error, unauthorized}.
db_info(_) -> {error, unauthorized}.
sync_hlc(_, _) -> {error, unauthorized}.
