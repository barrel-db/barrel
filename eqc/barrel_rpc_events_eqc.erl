%%% @author Zachary Kessin <>
%%% @copyright (C) 2017, Zachary Kessin
%%% @doc
%%% This is the main quickcheck property for barrel. It creats a sequence of events that is then
%%% run and compaired vs a model of what should happen.
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
					replicate = false :: false| binary(),
					deleted = sets:new()
         }).



%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)

db(#state{db = [DB|_]}) ->
		DB.

id() ->
    utf8(16).

doc()->
    #{
     <<"id">> => id(),
     <<"content">> => utf8(8)
    }.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Get Commands
%% 
%% This section tests getting data from the database. Each get will make a request 
%% From the server than compare the resulting document to a store kept by the test
%%


get_command(_S = #state{keys = Dict , replicate= R, db= DBS =[D|_]}) ->
		DB = case R of
						 {var, _} -> oneof(DBS);
						 false -> D
				 end,
    oneof([
           {call, barrel, get,       [DB, oneof(dict:fetch_keys(Dict)), #{}]},
           {call, barrel, get,       [DB, id(), #{}]},
					 {call, barrel, get,       [DB, oneof(dict:fetch_keys(Dict)), #{history => true }]}
          ]).

%% This function adapts tests so that replication state does not get screwed up
%% By shrinking and that we only look at the 2nd database if replication is active

get_adapt(#state{replicate = false, db = [D1,D2]}, [D2, Key, Meta])->
		{call, barrel, get, [D1, Key, Meta]};
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

get_post(#state{keys= Dict}, [_DB, Id, #{history := true}], {ok, Doc, Meta}) ->
		true; 

get_post(#state{keys= Dict}, [_DB, Id, #{}], {error, not_found}) ->
    not(dict:is_key(Id, Dict));
get_post(#state{keys= Dict}, [_DB, Id, #{}],
         {ok, Doc = #{<<"id">> := Id, <<"content">> := _Content} ,
          _rev}) ->
    {ok, Doc} == dict:find(Id, Dict).

%%********************************************************************************
%% This group turns on replication
%% In the case that replication is already on #state{replicate != false} it will not run
%%
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
%% Stop replication if it is turned on.

stop_replication_pre(#state{replicate = false}) ->
		false;
stop_replication_pre(#state{replicate = _S}) ->
		true.


stop_replication({ok, #{id := Id}}) -> 
		%timer:sleep(100),
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
%% Post documents to the server
%% Documents are crated randomly as are keys in the doc/0 function

post_post(#state{keys = Dict} , [_DB, #{<<"id">> := Id}, #{}], {error, {conflict, doc_exists}}) ->
    dict:is_key(Id, Dict);

post_post(_State, _Args, _Ret) ->
    true.

post_command(S = #state{keys = _Dict}) ->
    oneof([{call, barrel, post,  [db(S), doc(), #{}]}]).


post_next(State = #state{keys = Dict,cmds = C},{var, _N}, _Cmd = [_DB, Doc = #{<<"id">> := Id} , _opt]) ->
    State#state {keys = dict:store(Id, Doc, Dict), cmds= C + 1};
post_next(State = #state{keys = Dict,cmds = C},
          _V    = {ok, Id, _Rev},
          _Cmd  = [_DB, Doc = #{<<"id">> := Id}, _opt]) ->
    State#state{keys = dict:store(Id, Doc, Dict), cmds= C + 1}.


%%********************************************************************************
%% Update documents on the server
%% It will take an existing doc and udpate it
%%
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
%% Delete documents from the server

delete_pre(#state{keys = Dict}) ->
    not(dict:is_empty(Dict)).

delete_post(#state{keys= Dict},[_DB, Id,_] , {error,not_found}) ->
    not(dict:is_key(Id, Dict));
delete_post(#state{keys= Dict, deleted =D}, [_DB, Id, #{}], {ok, Id, _rev}) ->
		case {   dict:is_key(Id, Dict), sets:is_element(Id,D)} of
				{true, true} ->
						true;
				{true, false} ->
						true;
				{false, false} ->
						false;
				{false, true}->
						true
		end.


delete_command(S = #state{keys = Dict,deleted = D}) ->
    oneof([
           {call, barrel, delete,    [db(S), oneof(dict:fetch_keys(Dict)), #{}]},
           {call, barrel, delete,    [db(S), id(), #{}]},
					 {call, barrel, delete,    [db(S), oneof([<<"a">>|sets:to_list(D)]), #{}]}
          ]).


delete_next(State = #state{keys = Dict, cmds = C, deleted=D},_V,[_DB, Id|_]) ->
    State#state{keys = dict:erase(Id, Dict), cmds = C + 1, deleted = sets:add_element(Id,D)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Generator to update a document

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

%% Ensure that the number of documents is always posititve
-spec precondition_common(S, Call) -> boolean()
                                          when S    :: eqc_statem:symbolic_state(),
                                               Call :: eqc_statem:call().
precondition_common(#state{db = [DB|_], cmds = _N}, _Call) ->
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
postcondition_common(_S= #state{keys = Dict, db = [DB|_]}, _Call, _Res) ->
    case  barrel:database_infos(DB) of
        {error, not_found } -> 
						false;
        {ok, _A = #{docs_count :=DocCount}} ->
						DocCount >= dict:size(Dict)
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

stop(#state{replicate = S}) when is_binary(S) ->
		barrel:stop_replication(S);
stop(#state{}) ->
		ok.
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
											%	timer:sleep(250),
												
                        {H, S, Res} = run_commands(Cmds),
												stop(S),

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
