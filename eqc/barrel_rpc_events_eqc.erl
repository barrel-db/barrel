%%% @author Zachary Kessin <>
%%% @copyright (C) 2017, Zachary Kessin
%%% @doc
%%%
%%% @end
%%% Created :  6 Jul 2017 by Zachary Kessin <>

-module(barrel_rpc_events_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-define(IDField, <<"id">>).

-compile(export_all).
%% -- State and state functions ----------------------------------------------
-record(state,{
          db :: [binary()],
          cmds = 0:: integer(),
          keys = dict:new() :: dict:dict(binary(), term()),
          online = true :: boolean(),
          replicate = false :: false| binary(),
          deleted = sets:new(),
          replicate_push  = []
         }).

-record(doc, {id :: binary(),
              value = []
             }).

%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)

db(#state{db = [DB|_]}) ->
    DB.

dbr(#state{db = [DB|_], replicate = _}) ->
    DB;
dbr(#state{db = DBS, replicate = _R}) ->
    oneof(DBS).

id() ->
    utf8(6).

doc()->
    #{
     ?IDField => id(),
     <<"doc">>  =>non_empty(map(non_empty(binary()),binary())) 
    }.

doc1() ->
    #{
      <<"newcontent">> => utf8(14),
      <<"content">> => utf8(4)
     }.

%%********************************************************************************




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



get_command(_S = #state{keys = Dict , replicate= R, db= DBS =[D|_]}) ->
    DB = case R of
             {var, _} -> oneof(DBS);
             false -> D
         end,
    oneof([
           {call, ?MODULE, get,  
            [DB, 
             oneof(dict:fetch_keys(Dict)), 
             Dict, oneof(['history', 'nothing'])]},
           {call, ?MODULE, get,      
            [DB, id(), Dict, 'nothing']}
          ]).


%% get(DB, {ok, Id, _RevId}, _Dict, 'history') ->
%%     barrel:get(DB, Id, #{ 'history' => true});

%% get(DB, {ok, Id, _RevId}, _Dict, nothing) ->
%%     barrel:get(DB, Id, #{});


get({DBPid,DB}, Id , _Dict, 'history') ->
    case barrel:get({DBPid,DB}, Id, #{ 'history' => true}) of
        {ok, Doc, Meta} ->
            {Doc, Meta};
        {error, not_found} ->
            {error, not_found}
    end;

get({DBPid, DB}, Id, _Dict, _) ->
    case  barrel:get(DBPid, DB, Id, #{}) of 
        {ok, Doc, Meta} ->
            {Doc, Meta};
        {error, not_found} ->
            {error, not_found}
    end.


get_adapt(#state{replicate = false, db = [DB,_]}, [{_,<<"test02">>}|R])->
    {call, barrel, get, [DB|R]};
get_adapt(_S,_C) ->
    false.


get_pre(#state{replicate={var,_}}, [{_,<<"test02">>}|_]) ->
    false;
get_pre(#state{keys = Dict}, [_DB, Id|_]) ->
    dict:is_key(Id, Dict).


get_pre(#state{keys = Dict}) ->
    not(dict:is_empty(Dict)).

get_next(S=  #state{cmds = C},_,_) ->
    S#state{cmds = C + 1}.


get_post(#state{keys= Dict}, [_DB, Id,_, _], {error, not_found}) ->
    lager:error("Keys ~p  ~p", [Id, dict:fetch_keys(Dict)]),
    not(dict:is_key(Id, Dict));



get_post(#state{keys= Dict}, [_DB, Id,_, history], {_Doc, Meta}) ->
    Revisions = barrel_doc:parse_revisions(Meta),
    lager:error("--- revisions ~p", [Revisions]),
    case dict:find(Id, Dict) of
        {ok,#doc{value = Revs}} ->

            lists:all(
              fun({R,_} ) ->
                      lists:member(R, Revisions)
              end, Revs);
        error->
            true
    end;


get_post(#state{keys= Dict}, [_DB, Id|_ ],
         {_Doc = #{?IDField := Id, <<"content">> := _Content} ,
          #{<<"rev">> := Rev}}) ->


    {ok, #doc{id = Id,
              value = V}} = dict:find(Id, Dict),
    lager:notice("Rev ~s", [Rev]),
    [lager:notice("Rev Hist ~s", [S])|| {S,_} <- V],
    lists:keymember(Rev,1,V).



%%********************************************************************************

start_replication_pre(#state{replicate = R, db=_DBS}) ->
    R == false, false.

start_replication_command(#state{db = [DB1, DB2]}) ->
    oneof([{call, ?MODULE, start_replication,
            [DB1, DB2]}]).

start_replication(DB1, DB2) ->
    case barrel:start_replication(#{source => DB1,
                                    target=> DB2,
                                    options => #{ metrics_freq => 100 }}) of
        {ok, #{id := RepId}} ->
            RepId;
        _ ->
            {error, no_replication}
    end.

start_replication_next(State,  RepId,_) ->
%    lager:error("RepId ~p", [RepId]),
    State#state{replicate = RepId}.


start_replication_post(_,_,Id) when is_binary(Id)->
    true;
start_replication_post(_,_,_R) ->
    false.




%%********************************************************************************
stop_replication_pre(#state{replicate = false}) ->
    false;
stop_replication_pre(#state{replicate = _S}) ->
    true.



stop_replication( ok) ->
    ok;
stop_replication( Id) ->

    case barrel:stop_replication(Id) of
        ok ->
            ok;
        E ->

            E
    end.


stop_replication_command(#state{replicate = RepId}) ->
    oneof([{call, ?MODULE, stop_replication, [RepId]}]).

stop_replication_post(_,_,ok) ->
    true;
stop_replication_post(_,_,_Error) ->

    false.

stop_replication_next(State, R, _) ->
    State#state{replicate = false}.

%%********************************************************************************


post_post(#state{keys = Dict} , [_DB, #{?IDField := Id}, _],
          {conflict, doc_exists}) ->
    
    dict:is_key(Id, Dict),true;

post_post(_State, _Args,  {RevId,Doc} ) when is_binary(RevId) ->
    true.


post_command(S = #state{keys = _Dict}) ->
    oneof([
           {call, ?MODULE, post,  [dbr(S), doc(),  #{
                                                
                                               }]}
          ]).

post_pre(#state{replicate = false}, [<<"test02">>, _Doc, _Meta])->
    false;
post_pre(#state{keys=Dict},[_, Id|_]) ->
    not(dict:is_key(Id, Dict)). 


post ({DBPid, DB}, Doc = #{?IDField := Id}, Opts) ->
    lager:error("barrel:post(~p, ~p, ~p , ~p).~n", [DBPid, DB, Doc , Opts]),
    case barrel:post(DBPid, DB, Doc, Opts) of
        {ok, DocId, RevId} = R ->
            lager:error("Idx ~p", [R]),
            {RevId,Doc};
        {error, E} ->
            E
    end.

post_next(State = #state{keys = Dict,
                         cmds = C},
          Res ,
          _Cmd = [_DB, Doc = #{?IDField := Id} , _opt]) ->
    lager:error("++++ post next ~p", [Res]),
    case dict:is_key(Id, Dict) of
        true ->
            State;
        false ->
            State#state{keys = dict:store(Id,#doc{id = Id,
                                                  value = [Res]
                                                  }
                                         ,Dict)}
    end.


%%********************************************************************************



put_pre(#state{keys = Dict}) ->
    not(dict:is_empty(Dict)) .

   
put_pre(#state{replicate={var,_}}, [<<"test02">>|_]) ->
    false;
put_pre(#state{keys=Dict},[_, Id|_]) ->
    dict:is_key(Id, Dict). 



put_post(#state{keys = Dict} , [_DB, #{?IDField := Id}, #{}], {error, {conflict, doc_exists}}) ->
    dict:is_key(Id, Dict);

put_post(_State, _Args, _Ret) ->

    true.

update_doc(Dict) ->
    ?LET({Key, NewContent, N2 },
         {oneof(dict:fetch_keys(Dict)), utf8(4), utf8(16)},
         begin
             Doc = dict:fetch(Key, Dict),
             Doc#{<<"content">> => NewContent,
                  <<"new">> => N2}
         end).

put_command(S = #state{keys = Dict}) ->
    oneof([
           {call, ?MODULE, put,   [db(S),
                                   oneof(dict:fetch_keys(Dict)),
                                   doc1(),
                                   #{}]}
          ]).

put({DBPid, DB}, Id, Doc, Opts) ->
    {ok, Id, RevId} = barrel:put(DBPid, DB, Doc#{?IDField => Id}, Opts),
    lager:notice("Id ~p RevId ~s", [Id, RevId]),
    {RevId, Doc#{?IDField => Id}}.


put_next(State = #state{keys = Dict,cmds = C},
         Res ,
         _Cmd = [_DB, Id, Doc  , _opt]) ->


    {ok,NewDoc = #doc{value = RevDict}} = dict:find(Id, Dict),

    NewDictVal = [ Res|RevDict ],

    State#state{keys = dict:store(Id, NewDoc#doc{value = NewDictVal}, Dict), cmds= C + 1}.




%%********************************************************************************

put_rev_pre(#state{keys = Dict}) ->
    not(dict:is_empty(Dict)).


put_rev_pre(#state{replicate={var,_}}, [<<"test02">>|_]) ->
    false;
put_rev_pre(#state{keys=Dict},[_, Id|_]) ->
    dict:is_key(Id, Dict). 


put_rev_post(#state{keys = Dict} , [_DB, #{?IDField := Id}, #{},_], {error, {conflict, doc_exists}}) ->
    dict:is_key(Id, Dict);

put_rev_post(_State, _Args, _Ret) ->
    true.


put_rev_command(S = #state{keys = Dict}) ->
    oneof([
           {call, ?MODULE, put_rev,   [db(S),
                                       oneof(dict:fetch_keys(Dict)),
                                       doc1(),
                                       Dict,
                                       #{},
                                       choose(1,5)]}
          ]).


swap(DBS, Cmds) ->
    [swapDB(DBS, Cmd) || Cmd <- Cmds].
 
swapDB([DB1,DB2], Cmd = [_|Rest]) ->
    [DB2|Rest].
        

getByPos(Pos, L= [A|_]) ->
    Arr = array:from_list(L, A),
    array:get(Pos, Arr).


put_rev({DBPid, DB}, Id, Doc, Dict, Opts, NPos) ->
    RevId = case dict:find(Id, Dict) of
                error ->
                    throw({error, document_not_found, Id});
                {ok,#doc{value = L}} ->
                    {RevId1,_ } = getByPos(NPos,L),
                    RevId1
            end,

    {Pos, _}   = barrel_doc:parse_revision(RevId),
    NewDoc =  barrel_doc:make_doc(
                #{value => Doc, id => Id},
                RevId,
                false),
    NewRev = barrel_doc:revid(Pos + 1, RevId,NewDoc),

    History = [NewRev,RevId],

    {ok,Id, NewRev1} =  barrel:put_rev(DBPid, DB, Doc#{?IDField => Id}, History, false, Opts),
    lager:notice("Id ~p RevId ~s", [Id, NewRev1]),
    {NewRev1, Doc#{?IDField => Id}}.


put_rev_next(State = #state{cmds =C, replicate_push = P},
             Res,
             Cmd = [_DB, Id, Doc, Dict , _opt,_]) ->
    case dict:find(Id, Dict) of
        {ok, NewDoc= #doc{value = RevDict}} ->
            NewDictVal = lists:append([Res], RevDict),
            State#state{keys = dict:store(Id, NewDoc#doc{value = NewDictVal}, Dict),
                        replicate_push  = [Cmd|P],
                        cmds= C + 1}
    end.

%%********************************************************************************


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
           {call, ?MODULE, delete,    [db(S), oneof(dict:fetch_keys(Dict)), #{}]},
           {call, ?MODULE, delete,    [db(S), id(), #{}]},
           {call, ?MODULE, delete,    [db(S), oneof([<<"a">>|sets:to_list(D)]), #{}]}
          ]).

delete({DBPid, DB}, Key, Opts) ->
    barrel:delete(DBPid, DB, Key, Opts).

delete_next(State = #state{keys = Dict, cmds = C, deleted=D},_V,[_DB, Id|_]) ->
    State#state{keys = dict:erase(Id, Dict), cmds = C + 1, deleted = sets:add_element(Id,D)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% -- Generators -------------------------------------------------------------

%% -- Common pre-/post-conditions --------------------------------------------
%% @doc General command filter, checked before a command is generated.
-spec command_precondition_common(S, Cmd) -> boolean()
                                                 when S    :: eqc_statem:symbolic_state(),
                                                      Cmd  :: atom().
command_precondition_common(_S, _Cmd) ->
    true.

%%@doc General precondition, applied *before* specialized preconditions.
%% -spec precondition_common(S, Call) -> boolean()
%%                                           when S    :: eqc_statem:symbolic_state(),
%%                                                Call :: eqc_statem:call().
%% precondition_common(#state{db = [DB|_], cmds = _N}, _Call) ->
%%     case barrel:database_infos(DB) of
%%         {error,not_found} ->
%%             lager:error("DB NOT FOUND ~s ~p", [DB, _Call]),
%%             true;
%%         {ok, _A = #{docs_count := DocCount}} ->
%%             case DocCount >= 0 of
%%                 true -> true;
%%                 false ->
%%                     lager:error("Negative Doc Count ~p", [DocCount]),
%%                     true
%%             end
%%     end.







%% @doc General postcondition, applied *after* specialized postconditions.
-spec postcondition_common(S, Call, Res) -> true | term()
                                                when S    :: eqc_statem:dynamic_state(),
                                                     Call :: eqc_statem:call(),
                                                     Res  :: term().
postcondition_common(_S= #state{keys = Dict, db = [{DBPid, DB}|_]}, _Call, _Res) ->

    case  barrel:database_infos(DBPid,DB) of
        {error,not_found} ->
            false;
        {ok, _A = #{docs_count := DocCount}} ->

            case DocCount < 0
                of
                true ->
                    throw({error, negative_doc_count, DocCount});
                false ->
                    true
            end

    end;

postcondition_common(_,_,_) ->
    true.

%% -- Operations -------------------------------------------------------------



%% --- ... more operations

%% -- Property ---------------------------------------------------------------
%% @doc Default generated property



create_database(DB,RemoteNode) ->
    
    {ok, ChPid} = 
        barrel:connect(#{ type => direct, endpoint => RemoteNode }),
    
    case  barrel_remote:database_names(ChPid) of
        [] ->
            {ok, _} = barrel_remote:create_database(ChPid, #{ <<"database_id">> => DB }),

            ok;
        [DB] -> ok;
        Ds ->
            
            [begin 
                 ok = barrel_remote:delete_database(ChPid,D),
                 {ok, _} = barrel_remote:create_database(ChPid, #{ <<"database_id">> => DB }),
                 ok
             end
                 || D <- Ds]
    end,
    

    {ChPid, DB}.

cleanup() ->
    common_eqc:cleanup().

stop(#state{replicate = S}) when is_binary(S) ->
    barrel:stop_replication(S);
stop(#state{}) ->
    ok.

-spec prop_barrel_rpc_events_eqc() -> eqc:property().
prop_barrel_rpc_events_eqc() ->
    DBS = lists:zip([<<"test01">>,<<"test02">>], 
                    [node()|nodes()]),
    ?SETUP(fun common_eqc:init_db/0,
           ?FORALL( Cmds,
                    commands(?MODULE,
                             #state{
                                    db =[{var, db1},
                                         {var, db2}]
                               }),
                    begin
                        ?WHENFAIL(begin
                                      [ok = barrel:delete_database(D)|| D <-DBS],
                                      io:format("~n~n~n********************************************************************************~n~n~n"),
                                      ok
                                  end,
                                  begin
                                      Dbs = lists:zip([db1,db2],
                                                      [
                                                       create_database(DB, Node)||
                                                          {DB,Node} <- DBS]),

                                      {H, S, Res} = run_commands(Cmds,Dbs),
                                      stop(S),

                                      %[ok = barrel:delete_database(Conn,D)|| {D,Conn} <-DBS],
                                      check_command_names(Cmds,
                                                          measure(length, commands_length(Cmds),
                                                                  pretty_commands(?MODULE, Cmds, {H, S, Res},
                                                                                  Res == ok)))
                                  end)
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
