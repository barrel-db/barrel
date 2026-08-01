%%%-------------------------------------------------------------------
%%% @doc Backend resolution/selection tests for barrel_att_store.
%%% Covers backend_module/1, is_available/1, and open/2's clean failure
%%% on an unavailable backend -- deliberately not testing the s3 backend
%%% itself here (that needs the s3 profile and a live store; see
%%% barrel_att_s3's own test suite).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_store_backend_tests).

-include_lib("eunit/include/eunit.hrl").

backend_module_blob_test() ->
    ?assertEqual(barrel_att_store_blob, barrel_att_store:backend_module(blob)).

backend_module_s3_test() ->
    ?assertEqual(barrel_att_s3_store, barrel_att_store:backend_module(s3)).

backend_module_back_compat_bare_module_test() ->
    %% An already-resolved module atom passes through unchanged, for any
    %% caller that names a backend module directly rather than a symbolic
    %% atom (no such caller exists today, kept for back-compat).
    ?assertEqual(barrel_att_store_blob,
                 barrel_att_store:backend_module(barrel_att_store_blob)),
    ?assertEqual(some_custom_backend_module,
                 barrel_att_store:backend_module(some_custom_backend_module)).

is_available_blob_test() ->
    ?assert(barrel_att_store:is_available(blob)).

is_available_s3_matches_module_loadability_test() ->
    %% This suite runs under both the default profile (barrel_att_s3 not in
    %% the build) and the s3 profile (it is), so assert the invariant rather
    %% than a profile-specific answer: is_available/1 must truthfully reflect
    %% whether the backend module can actually be loaded.
    Expected = case code:ensure_loaded(barrel_att_s3_store) of
        {module, _} -> true;
        {error, _} -> false
    end,
    ?assertEqual(Expected, barrel_att_store:is_available(s3)).

is_available_bare_module_default_true_test() ->
    ?assert(barrel_att_store:is_available(barrel_att_store_blob)).

open_s3_backend_consistent_with_availability_test() ->
    %% Same profile-agnostic reasoning: under the default profile this
    %% asserts the clean {backend_unavailable, s3} failure; under the s3
    %% profile the module loads and (until Step 2 fills it in) open/2
    %% dispatches through to its not_implemented stub.
    Result = barrel_att_store:open("/tmp/does-not-matter", #{backend => s3}),
    case barrel_att_store:is_available(s3) of
        false -> ?assertEqual({error, {backend_unavailable, s3}}, Result);
        true -> ?assertEqual({error, not_implemented}, Result)
    end.

open_default_backend_still_works_test() ->
    Dir = "/tmp/barrel_att_store_backend_tests_"
        ++ integer_to_list(erlang:unique_integer([positive])),
    {ok, AttRef} = barrel_att_store:open(Dir, #{}),
    ?assertEqual(barrel_att_store_blob, maps:get(backend, AttRef)),
    ok = barrel_att_store:close(AttRef),
    os:cmd("rm -rf " ++ Dir).
