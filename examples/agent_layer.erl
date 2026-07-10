%%%-------------------------------------------------------------------
%%% @doc End-to-end example of the Barrel agent layer.
%%%
%%% Two agents share one space. Alice opens it, writes a document, keeps a
%%% session, then hands the task to Bob. Bob holds nothing but a capability
%%% token: accepting it opens the same space and gives him his own session.
%%% The context is read in place, never copied.
%%%
%%% Run it:
%%% ```
%%% $ rebar3 shell
%%% 1> c("examples/agent_layer.erl").
%%% 2> agent_layer:run().
%%% '''
%%%
%%% This module is compiled and executed by barrel_agent_example_SUITE, so
%%% it cannot drift from the API.
%%% @end
%%%-------------------------------------------------------------------
-module(agent_layer).

-export([run/0]).

run() ->
    {ok, _} = application:ensure_all_started(barrel_spaces),

    %% A space is a barrel database plus a registry entry. The label is
    %% metadata; the id is generated, so labels never constrain naming.
    {ok, #{id := SpaceId, db := Db} = Space} =
        barrel_spaces:create_space(#{label => <<"draft-review">>}),

    %% Inside a space, every barrel feature works unchanged.
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"draft">>,
                                   <<"title">> => <<"Design review">>,
                                   <<"body">> => <<"Ship the sync protocol">>}),

    %% Alice keeps her working context in a session.
    {ok, Alice} = barrel_session:create(Space, #{agent => <<"alice">>,
                                                 ttl => 3600}),
    {ok, _} = barrel_session:add_message(Space, Alice,
                                         #{role => <<"user">>,
                                           content => <<"tighten the intro">>}),
    {ok, _Rev} = barrel_session:set_data(Space, Alice, <<"cursor">>, 42),
    {ok, 42} = barrel_session:get_data(Space, Alice, <<"cursor">>),

    %% A capability is an opaque token, verified against a grant document.
    %% Only its SHA-256 is stored, so revocation is immediate and nothing on
    %% disk can mint access. The token is shown once, here.
    {ok, Token, _Grant} = barrel_caps:grant(SpaceId, #{rights => [read, write],
                                                       subject => <<"bob">>}),
    {ok, _Ctx} = barrel_caps:verify(Token, SpaceId, write),

    %% Hand the task over. Creating a handoff issues its own grant.
    {ok, #{handoff_id := Hid, token := HandoffToken}} =
        barrel_handoff:create(Space, #{task_name => <<"finish the draft">>,
                                       from_agent => <<"alice">>,
                                       to_agent => <<"bob">>,
                                       from_session => Alice,
                                       context => <<"intro still rough">>,
                                       pending => [<<"polish conclusion">>]}),

    %% Bob, holding only the token. accept/2 opens the shared space and
    %% creates his session in it.
    {ok, #{space := BobSpace, session := Bob, handoff := H}} =
        barrel_handoff:accept(HandoffToken, #{agent => <<"bob">>}),
    #{<<"handoff">> := Hid} = H,

    %% Same space, so Alice's document is already there.
    {ok, #{<<"title">> := <<"Design review">>}} =
        barrel:get_doc(maps:get(db, BobSpace), <<"draft">>),

    %% And Alice's messages are readable in place, not copied.
    {ok, [_ | _]} = barrel_session:get_messages(Space, Alice),

    {ok, _} = barrel_session:add_message(BobSpace, Bob,
                                         #{role => <<"assistant">>,
                                           content => <<"conclusion polished">>}),

    %% Completing revokes the handoff's grant by default. Note it takes the
    %% token, not the handoff id.
    {ok, _} = barrel_handoff:complete(HandoffToken, #{result => <<"shipped">>}),

    %% Alice's own grant is separate and still live until she revokes it.
    ok = barrel_caps:revoke(Token),
    {error, _} = barrel_caps:verify(Token, SpaceId, read),

    ok = barrel_session:delete(Space, Alice),
    ok = barrel_spaces:close_space(SpaceId),
    ok.
