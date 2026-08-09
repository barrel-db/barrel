%%%-------------------------------------------------------------------
%%% @doc Top supervisor for barrel_att_s3. The only static child is the
%%% multipart-upload GC sweeper (see barrel_att_s3_multipart_gc).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_s3_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @private
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 60
    },
    MultipartGc = #{
        id => barrel_att_s3_multipart_gc,
        start => {barrel_att_s3_multipart_gc, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker
    },
    {ok, {SupFlags, [MultipartGc]}}.
