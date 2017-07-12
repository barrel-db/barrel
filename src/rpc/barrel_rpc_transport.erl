%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. Jun 2017 15:41
%%%-------------------------------------------------------------------
-module(barrel_rpc_transport).
-author("benoitc").

%% barrel_gen_connection behaviour

-callback init() -> {ok, any()}.

-callback connect(
    Params :: map(), TypeSup :: pid(), State ::any()
) -> ok | {error, any()}.

-callback handle_message(
    Msg :: any(), State :: any()
) -> {ok, State :: any()} | {stop, Reason::any(), State :: any()}.

-callback terminate(
    Reason :: any(), State :: any()
) -> ok.
