%%%-------------------------------------------------------------------
%% @doc barrel_attachments top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(barrel_attachments_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
  EvictFun = fun(Path, _AttPid) ->
                 barrel_fs_att:evict(Path)
             end,

  AttLruOpts = [{max_objs, barrel_config:get(keep_attachment_file_num)},
                {evict_fun, EvictFun}],


  ChildSpecs = [
                #{ id => lru,
                   start => {lru, start_link, [{local, attachment_files}, AttLruOpts]}
                 },

                #{id => barrel_fs_att_sup,
                  start => {barrel_fs_att_sup, start_link, []}
                 }
               ],

  SupFlags = #{strategy => one_for_one,
               intensity => 4,
               period => 2000},

  {ok, {SupFlags, ChildSpecs}}.

%% internal functions
