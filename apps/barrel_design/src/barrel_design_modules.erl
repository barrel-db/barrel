%% @doc Node-wide holder of loaded guest modules.
%%
%% erlang_wasm keeps one decoded, validated module per SHA-256 for as
%% long as some process holds a load claim, and instances do not hold
%% one. This process holds the claim: users acquire a digest and release
%% it, and the module is unloaded when its last user has released it or
%% exited. Users release only after their in-flight guest calls have
%% finished, so unloading never races a call. A cache restart drops every
%% module; `reload/1' loads the digest again from the bytes kept here.
-module(barrel_design_modules).
-behaviour(gen_server).

-export([start_link/0, acquire/2, release/1, reload/1, info/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-record(entry, {handle, bytes, users = #{} :: #{pid() => pos_integer()}}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Load (or share) the module with this digest for the caller.
-spec acquire(binary(), binary()) -> {ok, term()} | {error, term()}.
acquire(Digest, Bytes) ->
    gen_server:call(?MODULE, {acquire, Digest, Bytes, self()}, 30000).

%% @doc Drop one of the caller's claims on Digest.
-spec release(binary()) -> ok.
release(Digest) ->
    gen_server:call(?MODULE, {release, Digest, self()}, 30000).

%% @doc Load the digest again after the engine lost it.
-spec reload(binary()) -> {ok, term()} | {error, term()}.
reload(Digest) ->
    gen_server:call(?MODULE, {reload, Digest}, 30000).

%% @doc Resident digests with their user counts, and the number of
%% engine loads this holder has made since it started.
-spec info() -> #{resident := #{binary() => non_neg_integer()}, loads := non_neg_integer()}.
info() ->
    gen_server:call(?MODULE, info).

init([]) ->
    {ok, #{entries => #{}, monitors => #{}, loads => 0}}.

handle_call({acquire, Digest, Bytes, Pid}, _From,
            #{entries := Entries} = State) ->
    case maps:find(Digest, Entries) of
        {ok, #entry{handle = H, users = U} = E} ->
            State1 = watch(Pid, State),
            {reply, {ok, H}, State1#{entries := Entries#{Digest => E#entry{users = bump(Pid, U)}}}};
        error ->
            case load(Digest, Bytes, State) of
                {ok, H, State1} ->
                    E = #entry{handle = H, bytes = Bytes, users = #{Pid => 1}},
                    State2 = watch(Pid, State1),
                    {reply, {ok, H}, State2#{entries := Entries#{Digest => E}}};
                {error, _} = Error ->
                    {reply, Error, State}
            end
    end;
handle_call({release, Digest, Pid}, _From, State) ->
    {reply, ok, drop(Digest, Pid, 1, State)};
handle_call({reload, Digest}, _From, #{entries := Entries} = State) ->
    case maps:find(Digest, Entries) of
        {ok, #entry{handle = Old, bytes = Bytes} = E} ->
            %% drops our claim if the engine still has one, so the load
            %% below leaves exactly one
            _ = wasm:unload(Old),
            case load(Digest, Bytes, State) of
                {ok, H, State1} ->
                    {reply, {ok, H}, State1#{entries := Entries#{Digest => E#entry{handle = H}}}};
                {error, _} = Error ->
                    {reply, Error, State}
            end;
        error ->
            {reply, {error, not_acquired}, State}
    end;
handle_call(info, _From, #{entries := Entries, loads := Loads} = State) ->
    Resident = maps:map(fun(_, #entry{users = U}) -> lists:sum(maps:values(U)) end, Entries),
    {reply, #{resident => Resident, loads => Loads}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, Pid, _}, #{monitors := Mons, entries := Entries} = State) ->
    State1 = State#{monitors := maps:remove(Pid, Mons)},
    _ = Ref,
    State2 = maps:fold(fun(Digest, #entry{users = U}, Acc) ->
        case maps:find(Pid, U) of
            {ok, N} -> drop(Digest, Pid, N, Acc);
            error -> Acc
        end
    end, State1, Entries),
    {noreply, State2};
handle_info(_Info, State) ->
    {noreply, State}.

load(Digest, Bytes, #{loads := Loads} = State) ->
    case wasm:load(Bytes) of
        {ok, {wasm_module, Digest} = H} -> {ok, H, State#{loads := Loads + 1}};
        {ok, Other} ->
            _ = wasm:unload(Other),
            {error, {digest_mismatch, Digest}};
        {error, E} -> {error, {load_failed, Digest, E}}
    end.

drop(Digest, Pid, N, #{entries := Entries} = State) ->
    case maps:find(Digest, Entries) of
        {ok, #entry{handle = H, users = U} = E} ->
            U1 = case maps:get(Pid, U, 0) - N of
                Left when Left > 0 -> U#{Pid => Left};
                _ -> maps:remove(Pid, U)
            end,
            case map_size(U1) of
                0 ->
                    _ = wasm:unload(H),
                    State#{entries := maps:remove(Digest, Entries)};
                _ ->
                    State#{entries := Entries#{Digest => E#entry{users = U1}}}
            end;
        error ->
            State
    end.

watch(Pid, #{monitors := Mons} = State) ->
    case maps:is_key(Pid, Mons) of
        true -> State;
        false -> State#{monitors := Mons#{Pid => erlang:monitor(process, Pid)}}
    end.

bump(Pid, U) -> maps:update_with(Pid, fun(N) -> N + 1 end, 1, U).
