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
          db :: [binary()],
          cmds = 0:: integer(),
          keys:: dict:dict(binary(), term()),
          online = true :: boolean(),
					replicate = false :: false| binary()
         }).



%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)

db(#state{db= [DB|_]}) ->
		DB.

id() ->
    utf8(16).

doc()->
    #{
     <<"id">> => id(),
     <<"content">> => utf8(8)
    }.

%%********************************************************************************                                                
%% Validate the results of a call



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_command(S= #state{keys = Dict, replicate={var,_}, db= DBS}) ->
		oneof([
					 {call, barrel, get,       [oneof(DBS), oneof(dict:fetch_keys(Dict)), #{}]},
           {call, barrel, get,       [oneof(DBS), id(), #{}]}
					]);
get_command(S= #state{keys = Dict}) ->
    oneof([
           {call, barrel, get,       [db(S), oneof(dict:fetch_keys(Dict)), #{}]},
           {call, barrel, get,       [db(S), id(), #{}]}
          ]).

get_adapt(#state{replicate = false}, [<<"test02">>, Key, Meta])->
 		lager:error("+++++New Command ~p~n",[		{call, barrel, get, [<<"test01">>, Key, Meta]}]),
		{call, barrel, get, [<<"test01">>, Key, Meta]};
get_adapt(_S,_C) ->
		false.


get_pre(#state{replicate={var,_}}, [<<"test02">>, _, _]) ->
		false;
get_pre(_,_) ->
		true.

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

%%********************************************************************************

start_replication_pre(#state{replicate = R, db=_DBS}) ->
 		R == false.

start_replication_command(#state{db = [DB1, DB2]}) ->
		oneof([{call, barrel, start_replication,
						[#{source => DB1,
							 target=> DB2,
							 options => #{ metrics_freq => 100 }}]}]).

start_replication_next(State, _v = {ok, #{id := RepId}},_) -> 
		State#state{replicate = RepId};
start_replication_next(State,N = {var,_} ,_) ->

		State#state{replicate = N};
start_replication_next(S,_,_) ->
		S.


start_replication_post(_,_,{ok, #{id := Id}}) when is_binary(Id)->

		true;
start_replication_post(_,_,_R) ->

		true.




%%********************************************************************************
stop_replication_pre(#state{replicate = false}) ->
		false;
stop_replication_pre(#state{replicate = _S}) ->

		true.


stop_replication({ok, #{id := Id}}) -> 
		timer:sleep(100),
		barrel:stop_replication(Id).

stop_replication_command(#state{replicate = RepId}) ->
		oneof([{call, ?MODULE, stop_replication, [RepId]}]).

stop_replication_post(_,_,ok) ->
		true;
stop_replication_post(_,_,_Error) ->
		false.

stop_replication_next(State= #state{replicate = {var,_}}, {var,_}, _) ->
		State#state{replicate = false};
stop_replication_next(State, ok,_) ->

 		State#state{replicate = false};
stop_replication_next(State, _,_) ->

		State.

%%********************************************************************************


post_post(#state{keys = Dict} , [_DB, #{<<"id">> := Id}, #{}], {error, {conflict, doc_exists}}) ->
    dict:is_key(Id, Dict);

post_post(_State, _Args, _Ret) ->
    true.

post_command(S = #state{keys = Dict}) ->
    oneof([{call, barrel, post,  [db(S), doc(), #{}]}]).


post_next(State = #state{keys = Dict,cmds = C},{var, N},_Cmd = [_DB, Doc = #{<<"id">> := Id} , _opt]) ->
    State#state {keys = dict:store(Id, Doc, Dict), cmds= C + 1};
post_next(State = #state{keys = Dict,cmds = C},
          _V    = {ok, Id, _Rev},
          _Cmd  = [_DB, Doc = #{<<"id">> := Id}, _opt]) ->
    State#state{keys = dict:store(Id, Doc, Dict), cmds= C + 1}.


%%********************************************************************************

put_pre(#state{keys = Dict}) ->
    not(dict:is_empty(Dict)).

put_post(#state{keys = Dict} , [_DB, #{<<"id">> := Id}, #{}], {error, {conflict, doc_exists}}) ->
    dict:is_key(Id, Dict);

put_post(_State, _Args, _Ret) ->
    true.

put_command(S = #state{keys = Dict}) ->
    oneof([
           {call, barrel, put,   [db(S), update_doc(Dict), #{}]}
          ]).

put_next(State = #state{keys = Dict,cmds = C},
				 {var,_N},
				 _Cmd = [_DB, Doc = #{<<"id">> := Id} , _opt]) ->
    State#state {keys = dict:store(Id, Doc, Dict), cmds= C + 1};
put_next(State = #state{keys = Dict,cmds = C},
         _V   = {ok, Id, _Rev},
         _Cmd = [_DB, Doc = #{<<"id">> := Id}, _opt]) ->
    State#state{keys = dict:store(Id, Doc, Dict), cmds= C + 1};
put_next(State, _,_) ->
		State.




%%********************************************************************************


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
    ?LET({Key, NewContent, N2 },
         {oneof(dict:fetch_keys(Dict)), utf8(4), utf8(16)},
         begin
             {ok, Doc1} = dict:find(Key, Dict),
             Doc1#{<<"content">> => NewContent, 
                   <<"new">> => N2}
         end).


%% -- Generators -------------------------------------------------------------

%% -- Common pre-/post-conditions --------------------------------------------
%% @doc General command filter, checked before a command is generated.
-spec command_precondition_common(S, Cmd) -> boolean()
                                                 when S    :: eqc_statem:symbolic_state(),
                                                      Cmd  :: atom().
command_precondition_common(_S, _Cmd) ->
    true.

%% @doc General precondition, applied *before* specialized preconditions.
%% -spec precondition_common(S, Call) -> boolean()
%%                                           when S    :: eqc_statem:symbolic_state(),
%%                                                Call :: eqc_statem:call().
%% precondition_common(#state{db = DB, cmds = _N}, _Call) ->
%%     case barrel:database_infos(DB) of
%%         {error,not_found} -> true;
%%         {ok, _A = #{docs_count := DocCount}} ->
%% 						DocCount >= 0
%%     end.







%% @doc General postcondition, applied *after* specialized postconditions.
-spec postcondition_common(S, Call, Res) -> true | term()
                                                when S    :: eqc_statem:dynamic_state(),
                                                     Call :: eqc_statem:call(),
                                                     Res  :: term().
postcondition_common(_S= #state{keys = _Dict, db = [DB|_]}, _Call, _Res) ->

    case  barrel:database_infos(DB) of
        {error, not_found } -> 
						io:format("Database Not found ~p~n", [DB]),
						false;
        {ok, _A = #{docs_count := _DocCount}} ->
            true
						%%DocCount >= dict:size(Dict)
    end;

postcondition_common(_,_,_) ->
    true.

%% -- Operations -------------------------------------------------------------



%% --- ... more operations

%% -- Property ---------------------------------------------------------------
%% @doc Default generated property



create_database(DB) ->
    case barrel:database_infos(DB) of
        {ok, _} -> barrel:delete_database(DB);
        _ -> ok
    end,
    barrel:create_database(#{<<"database_id">> => DB}).



cleanup() ->
    common_eqc:cleanup().

%% uuid() ->
%%     list_to_binary(uuid:uuid_to_string(uuid:get_v4_urandom())).

-spec prop_barrel_rpc_events_eqc() -> eqc:property().
prop_barrel_rpc_events_eqc() ->
    ?SETUP(fun common_eqc:init_db/0,
           ?FORALL( Cmds,
                    commands(?MODULE,
                             #state{keys = dict:new(),
                                    db =  [<<"test01">>,<<"test02">>]
                                   }),
                    begin
                        [{model, ?MODULE},
                         {init, #state{db= DBS}}
												 |_ ] 
														= Cmds,
												[begin
														 {ok,#{<<"database_id">> := D}} = create_database(D)
												 end
												 || D <-DBS],
												timer:sleep(250),
												
                        {H, S, Res} = run_commands(Cmds),
												R = S#state.replicate,
												case R of
														S when is_binary(S) ->
																barrel:stop_replication(S);
														_ ->ok
												end,
																
																				 
												[ok = barrel:delete_database(D)|| D <-DBS],
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
