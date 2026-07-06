%%%-------------------------------------------------------------------
%%% @doc Janitor for session leftovers: when the space's TTL sweeper
%%% tombstones an idle session doc, its message docs (which carry no
%%% TTL of their own) become orphans. The janitor periodically walks
%%% each OPEN space's `session:' prefix and deletes message groups
%%% whose session root is gone. Bounded per pass; deletions are real
%%% tombstones.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_spaces_janitor).
-behaviour(gen_server).

-export([start_link/0, sweep/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(DEFAULT_INTERVAL, 300000).
-define(MAX_DELETES_PER_PASS, 512).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Run one janitor pass synchronously (test hook; the periodic
%% timer runs the same code). Returns the number of deleted docs.
-spec sweep() -> {ok, non_neg_integer()}.
sweep() ->
    gen_server:call(?MODULE, sweep, infinity).

%%====================================================================
%% gen_server
%%====================================================================

init([]) ->
    arm(),
    {ok, #{}}.

handle_call(sweep, _From, State) ->
    {reply, {ok, do_sweep()}, State};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(sweep, State) ->
    _ = do_sweep(),
    arm(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%====================================================================
%% Internal
%%====================================================================

arm() ->
    Interval = application:get_env(barrel_spaces, janitor_interval,
                                   ?DEFAULT_INTERVAL),
    erlang:send_after(Interval, self(), sweep),
    ok.

do_sweep() ->
    Spaces = [Name || Name <- barrel_dbs:list(),
                      binary:part(Name, 0, min(3, byte_size(Name)))
                          =:= <<"sp_">>],
    lists:foldl(fun(SpaceDb, Deleted) ->
                    Deleted + sweep_space(SpaceDb)
                end, 0, Spaces).

%% a space can close between list and fold: skip it, next pass sees it
sweep_space(SpaceDb) ->
    try barrel_docdb:fold_docs(
            SpaceDb,
            fun(Doc, {Live, Dead, Acc}) ->
                case length(Acc) >= ?MAX_DELETES_PER_PASS of
                    true ->
                        {stop, {Live, Dead, Acc}};
                    false ->
                        classify(Doc, SpaceDb, Live, Dead, Acc)
                end
            end, {sets:new(), sets:new(), []},
            #{id_prefix => <<"session:">>}) of
        {ok, {_Live, _Dead, Orphans}} ->
            lists:foreach(
                fun(DocId) ->
                    _ = barrel_docdb:delete_doc(SpaceDb, DocId)
                end, Orphans),
            length(Orphans)
    catch
        _:_ ->
            0
    end.

%% roots sort before their message group, so live roots are usually in
%% the set already; dead roots are probed once per group
classify(#{<<"type">> := <<"session">>, <<"session">> := Sid},
         _SpaceDb, Live, Dead, Acc) ->
    {ok, {sets:add_element(Sid, Live), Dead, Acc}};
classify(#{<<"type">> := <<"message">>, <<"session">> := Sid,
           <<"id">> := DocId}, SpaceDb, Live, Dead, Acc) ->
    case sets:is_element(Sid, Live) of
        true ->
            {ok, {Live, Dead, Acc}};
        false ->
            case sets:is_element(Sid, Dead) of
                true ->
                    {ok, {Live, Dead, [DocId | Acc]}};
                false ->
                    case barrel_docdb:get_doc(
                             SpaceDb, <<"session:", Sid/binary>>) of
                        {ok, _} ->
                            {ok, {sets:add_element(Sid, Live), Dead,
                                  Acc}};
                        {error, _} ->
                            {ok, {Live, sets:add_element(Sid, Dead),
                                  [DocId | Acc]}}
                    end
            end
    end;
classify(_Doc, _SpaceDb, Live, Dead, Acc) ->
    {ok, {Live, Dead, Acc}}.
