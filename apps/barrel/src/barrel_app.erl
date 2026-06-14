%%%-------------------------------------------------------------------
%%% @doc barrel application module.
%%%
%%% barrel is the embeddable edge database facade. It composes the
%%% document layer (barrel_docdb) and the vector layer (barrel_vectordb)
%%% behind one API. Databases are opened on demand via {@link barrel:open/2},
%%% so the top supervisor has no static children.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_app).
-behaviour(application).

-export([start/2, stop/1]).

%% @private
start(_StartType, _StartArgs) ->
    barrel_sup:start_link().

%% @private
stop(_State) ->
    ok.
