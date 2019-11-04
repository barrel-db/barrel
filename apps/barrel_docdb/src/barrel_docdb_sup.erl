%%%-------------------------------------------------------------------
%% @doc barrel_docdb top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(barrel_docdb_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
  SupFlags = #{strategy => one_for_one,
               intensity => 4,
               period => 2000},
  ChildSpecs = [
                #{ id => local_epoch_store,
                   start => {barrel_local_epoch_store, start_link, []} },

                %% changes stream server
                #{ id => barrel_changes_streams,
                   start => {barrel_changes_streams, start_link, []}
                 },
                %% barrel db supervisor
                #{id => barrel_db_sup,
                  start => {barrel_db_sup, start_link, []}
                 },
                %% barrel view supervisor
                #{id => barrel_fold_process_sup,
                  start => {barrel_fold_process_sup, start_link, []}
                 }


               ],
  {ok, {SupFlags, ChildSpecs}}.

%% internal functions
