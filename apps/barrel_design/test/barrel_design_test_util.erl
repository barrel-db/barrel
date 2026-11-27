%% Shared fixtures for the barrel_design suites.
-module(barrel_design_test_util).

-export([start/1, start/2, stop/0, guest/1, invoice/5, all_rows/2, all_rows/3,
         recompute/2, recompute/3, normalized_view/2, wait_until/2, wait_caught_up/2,
         guest_calls/0, design/3]).

-include_lib("common_test/include/ct.hrl").

start(Config) -> start(Config, #{}).

start(Config, DesignEnv) ->
    Priv = ?config(priv_dir, Config),
    _ = application:load(barrel_docdb),
    ok = application:set_env(barrel_docdb, data_dir, filename:join(Priv, "docdb")),
    _ = application:load(barrel_design),
    ok = application:set_env(barrel_design, data_dir, filename:join(Priv, "design")),
    maps:foreach(fun(K, V) -> ok = application:set_env(barrel_design, K, V) end, DesignEnv),
    {ok, _} = application:ensure_all_started(barrel_design),
    ok.

stop() ->
    _ = application:stop(barrel_design),
    _ = application:stop(barrel_docdb),
    ok.

guest(Name) ->
    Here = filename:dirname(code:which(?MODULE)),
    Dirs = [filename:join(Here, "guests"),
            filename:join([Here, "..", "..", "..", "..", "apps", "barrel_design", "test", "guests"])],
    [Path | _] = [P || D <- Dirs, P <- [filename:join(D, Name)], filelib:is_file(P)],
    {ok, Bin} = file:read_file(Path),
    Bin.

invoice(Id, Customer, Status, Due, Lines) ->
    #{<<"id">> => Id, <<"type">> => <<"invoice">>, <<"customer">> => Customer,
      <<"status">> => Status, <<"due_on">> => Due, <<"line_cents">> => Lines}.

design(Name, Version, Views) ->
    #{name => Name, version => Version, views => Views}.

%% Every row of the view's current generation, paging with continuations.
all_rows(Db, View) -> all_rows(Db, View, #{}).

all_rows(Db, View, Opts) ->
    page(Db, View, Opts#{reduce => false, limit => 7}, []).

page(Db, View, Opts, Acc) ->
    case barrel_design:query(Db, View, Opts) of
        {ok, Rows, #{continuation := C}} -> page(Db, View, Opts#{continuation => C}, Acc ++ Rows);
        {ok, Rows, _} -> {ok, Acc ++ Rows};
        {error, _} = Error -> Error
    end.

%% The rows a clean build of a normalized view gives over the database's
%% live documents, in index order.
recompute(Db, View) -> recompute(Db, View, undefined).

recompute(Db, #{select := Select} = View, Module) ->
    {ok, Sel} = barrel_design_manifest:selector(Select),
    {ok, Docs} = barrel_docdb:fold_docs(Db, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []),
    Selected = [maps:remove(<<"_rev">>, D) || D <- lists:reverse(Docs),
                                               barrel_design_manifest:selected(Sel, D)],
    Results = case View of
        #{map := native} -> [barrel_design_native:map(View, D) || D <- Selected];
        #{map := #{export := E, fields := F}} ->
            Spec = #{export => E, fields => F, key => maps:get(key, View),
                     value => maps:get(value, View), limits => maps:get(limits, View)},
            barrel_design_guest:run(Module, Spec, Selected, #{})
    end,
    Rows = lists:append([[{barrel_design_key:encode(K), barrel_design_key:encode(Id), Ord,
                           #{id => Id, key => K, value => V}}
                          || {Ord, {K, V}} <- lists:enumerate(0, R)]
                         || {#{<<"id">> := Id}, {ok, R}} <- lists:zip(Selected, Results)]),
    [Row || {_, _, _, Row} <- lists:sort(Rows)].

normalized_view(Design, Name) ->
    {ok, #{views := #{Name := V}}} = barrel_design_manifest:normalize(Design),
    V.

wait_until(Fun, Ms) when Ms =< 0 ->
    case Fun() of true -> ok; false -> {error, timeout} end;
wait_until(Fun, Ms) ->
    case Fun() of
        true -> ok;
        false -> receive after 10 -> wait_until(Fun, Ms - 10) end
    end.

%% Wait, by observing status only (no refresh, no freshness read), until
%% the view's current generation covers the database's last write.
wait_caught_up(Db, View) ->
    {ok, #{last_seq := Target}} = barrel_docdb:db_observed_version(Db),
    ok = wait_until(fun() ->
        case barrel_design:status(Db, View) of
            {ok, #{status := complete, checkpoint := #{last_seq := Cp}} = S} ->
                Cp >= Target andalso not maps:is_key(rebuilding, S);
            _ -> false
        end
    end, 20000).

guest_calls() ->
    maps:get(guest_calls, barrel_design_stats:get()).
