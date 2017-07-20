%%% @author Zachary Kessin <>
%%% @copyright (C) 2017, Zachary Kessin
%%% @doc
%%%
%%% @end
%%% Created :  6 Jul 2017 by Zachary Kessin <>

-module(barrel_rpc_events_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-compile(export_all).
%% -- State and state functions ----------------------------------------------
-record(state,{
          db :: binary(),
          cmds = 0:: integer(),
          keys:: dict:dict(binary(), term()),
          online = true :: boolean()

              }).



%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{keys = dict:new() , db = uuid:get_v4_urandom()}.


db(#state{db= DB}) ->
    DB.


id() ->
    utf8(16).


doc()->
    #{
     <<"id">> => id(),
     <<"content">> => utf8(8)
    }.




%********************************************************************************
% Validate the results of a call

create_database_pre(#state{cmds = 0}) ->
    true;
create_database_pre(#state{cmds = N}) when N > 0->
    false.

create_database_next(S=  #state{cmds = C},_,_) ->
    S#state{cmds = C + 1}.

create_database_command(#state{cmds = 0, db = DB}) ->
    oneof([{call, ?MODULE, create_database, [ DB]}]).

create_database(DB) ->
    case barrel:database_infos(DB) of
        {ok, _} -> barrel:delete_database(DB);
        _ -> ok
    end,
    barrel:create_database(#{<<"database_id">> => DB}).


                                                %********************************************************************************

get_command(S= #state{keys = Dict}) ->
    oneof([
           {call, barrel, get,       [db(S), oneof(dict:fetch_keys(Dict)), #{}]},
           {call, barrel, get,       [db(S), id(), #{}]}
          ]).

get_pre(#state{cmds = 0}) ->false;
get_pre(#state{keys = Dict}) ->
    not(dict:is_empty(Dict)).

get_next(S=  #state{cmds = C},_,_) ->
    S#state{cmds = C + 1}.


get_post(#state{keys= Dict}, [_DB, Id, #{}], {error, not_found}) ->

    not(dict:is_key(Id, Dict));
get_post(#state{keys= Dict}, [_DB, Id, #{}],
         {ok, Doc = #{<<"id">> := Id, <<"content">> := _Content} ,
          _rev}) ->
    {ok, Doc} == dict:find(Id, Dict).


                                                %********************************************************************************
post_pre(#state{cmds = 0}) ->false;
post_pre(_S) ->
    true.


post_post(#state{keys = Dict} , [_DB, #{<<"id">> := Id}, #{}], {error, {conflict, doc_exists}}) ->
    dict:is_key(Id, Dict);

post_post(_State, _Args, _Ret) ->

    true.

post_command(S = #state{keys = Dict}) ->

    case dict:is_empty(Dict) of
        true ->
            oneof([{call, barrel, post,  [db(S), doc(), #{}]}]);
        false ->
            oneof([
                   {call, barrel, post,  [db(S), doc(), #{}]},
                   {call, barrel, put,   [db(S), doc(), #{}]},
                   {call, barrel, post,  [db(S), update_doc(Dict), #{}]},
                   {call, barrel, put,   [db(S), update_doc(Dict), #{}]}
                  ]
                 )
    end.




post_next(State = #state{keys = Dict,cmds = C},_V,[_DB, Doc = #{<<"id">> := Id} |_]) ->
    case dict:is_key(Id, Dict) of
        true ->
            State#state {keys = dict:store(Id, Doc, Dict), cmds= C + 1};
        false ->
            State#state {keys = dict:store(Id, Doc, Dict), cmds = C + 1}
    end.


                                                %********************************************************************************

delete_pre(#state{cmds = 0}) ->false;
delete_pre(#state{keys = Dict}) ->
    not(dict:is_empty(Dict)).

delete_post(#state{keys= Dict},[_DB, Id,_] , {error,not_found}) ->
    not(dict:is_key(Id, Dict));
delete_post(#state{keys= Dict}, [_DB, Id, #{}], {ok, Id, _rev}) ->
    dict:is_key(Id, Dict).

delete_command(S = #state{keys = Dict}) ->
    oneof([
           {call, barrel, delete,    [db(S), oneof(dict:fetch_keys(Dict)), #{}]},
           {call, barrel, delete,    [db(S), id(), #{}]}
          ]).


delete_next(State = #state{keys = Dict, cmds = C},_V,[_DB, Id|_]) ->
    State#state{keys = dict:erase(Id, Dict), cmds = C + 1}.


update_doc(Dict) ->
    ?LET({Key, NewContent},
         {oneof(dict:fetch_keys(Dict)), utf8(9)},
         begin
             {ok, Doc1} = dict:find(Key, Dict),
             Doc1#{<<"content">> => NewContent}

         end).




%% offline_pre(#state{online= Online}) ->
%%      not(Online).

%% offline_command(_S) ->
%%       [].

%% -- Generators -------------------------------------------------------------

%% -- Common pre-/post-conditions --------------------------------------------
%% @doc General command filter, checked before a command is generated.
-spec command_precondition_common(S, Cmd) -> boolean()
                                                 when S    :: eqc_statem:symbolic_state(),
                                                      Cmd  :: atom().
command_precondition_common(_S, _Cmd) ->
    true.

%% @doc General precondition, applied *before* specialized preconditions.
-spec precondition_common(S, Call) -> boolean()
                                          when S    :: eqc_statem:symbolic_state(),
                                               Call :: eqc_statem:call().
precondition_common(#state{ cmds = 0}, _Call) ->
    true;
precondition_common(#state{ cmds = 1}, _Call) ->
    true;
precondition_common(#state{db = DB, cmds = N}, _Call) ->
    case barrel:database_infos(DB) of
        {error,not_found} -> true;
        {ok, _A = #{docs_count := DocCount}} ->
            DocCount >= 0
    end.






%% @doc General postcondition, applied *after* specialized postconditions.
-spec postcondition_common(S, Call, Res) -> true | term()
                                                when S    :: eqc_statem:dynamic_state(),
                                                     Call :: eqc_statem:call(),
                                                     Res  :: term().
postcondition_common(_S= #state{keys = _Dict, cmds = 0}, _Call, _Res) ->
    true;
postcondition_common(_S= #state{keys = Dict, db = DB}, _Call, _Res) ->

    case  barrel:database_infos(DB) of
        {error, not_found } -> false;
        {ok, _A = #{docs_count := DocCount}} ->
            io:format("DocCount ~p ~p~n~n~n",[DocCount, dict:size(Dict)]),
            DocCount >= dict:size(Dict)
    end;

postcondition_common(_,_,_) ->
    true.

%% -- Operations -------------------------------------------------------------



%% --- ... more operations

%% -- Property ---------------------------------------------------------------
%% @doc Default generated property



cleanup() -> 
    common_eqc:cleanup().


-spec prop_barrel_rpc_events_eqc() -> eqc:property().
prop_barrel_rpc_events_eqc() ->
    ?SETUP(fun common_eqc:init_db/0,
           ?FORALL( Cmds,
                    commands(?MODULE,
                             #state{keys = dict:new() ,
                                    db = <<"test4">> % uuid:get_v4_urandom()
                                   }),
                    begin
                        cleanup(),
                        {H, S, Res} = run_commands(Cmds),
                        DB = S#state.db,

                        ?WHENFAIL(begin
                                      cleanup(),
                                      ok

                                  end,
                                  check_command_names(Cmds,
                                                      measure(length, commands_length(Cmds),
                                                              pretty_commands(?MODULE, Cmds, {H, S, Res},
                                                                              Res == ok))))
                    end)).

%% @doc Run property repeatedly to find as many different bugs as
%% possible. Runs for 10 seconds before giving up finding more bugs.
-spec bugs() -> [eqc_statem:bug()].
bugs() -> bugs(10).

%% @doc Run property repeatedly to find as many different bugs as
%% possible. Runs for N seconds before giving up finding more bugs.
-spec bugs(non_neg_integer()) -> [eqc_statem:bug()].
bugs(N) -> bugs(N, []).

%% @doc Run property repeatedly to find as many different bugs as
%% possible. Takes testing time and already found bugs as arguments.
-spec bugs(non_neg_integer(), [eqc_statem:bug()]) -> [eqc_statem:bug()].
bugs(Time, Bugs) ->
    more_bugs(eqc:testing_time(Time, prop_barrel_rpc_events_eqc()), 20, Bugs).
