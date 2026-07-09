%%%-------------------------------------------------------------------
%%% @doc The MCP resource templates: `barrel://db/{db}' (database
%%% info), `barrel://db/{db}/doc/{id}' (a document body), and
%%% `barrel://db/{db}/live/{sub}' (the materialized snapshot of a
%%% live query, see barrel_server_mcp_live).
%%%
%%% Resource read handlers are arity 1 in the framework: no auth
%%% context reaches them, so the db and doc templates expose reads to
%%% ANY authenticated MCP caller by URI. Deployments that hand out
%%% capability tokens should set `{barrel_server, mcp, #{resources =>
%%% live_only}}', which registers only the live template: live URIs
%%% carry a 16-byte random sub id, so possession of the URI is the
%%% capability.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_mcp_resources).

-export([register_all/0, unregister_all/0]).

%% Resource handlers (invoked by the barrel_mcp registry as M:F(Vars))
-export([db_resource/1, doc_resource/1, live_resource/1]).

%%====================================================================
%% Registration
%%====================================================================

register_all() ->
    lists:foreach(
        fun({Name, Fun, Opts}) ->
            ok = barrel_mcp:unreg_resource_template(Name),
            ok = barrel_mcp:reg_resource_template(Name, ?MODULE, Fun, Opts)
        end, specs(mode())).

unregister_all() ->
    %% unregister every known template whatever the current mode
    lists:foreach(
        fun({Name, _Fun, _Opts}) ->
            barrel_mcp:unreg_resource_template(Name)
        end, specs(full)).

mode() ->
    Cfg = application:get_env(barrel_server, mcp, #{}),
    maps:get(resources, Cfg, full).

specs(live_only) ->
    [live_spec()];
specs(_Full) ->
    [
        {<<"barrel_db">>, db_resource, #{
            uri_template => <<"barrel://db/{db}">>,
            description => <<"Info about a barrel database.">>,
            mime_type => <<"application/json">>
        }},
        {<<"barrel_doc">>, doc_resource, #{
            uri_template => <<"barrel://db/{db}/doc/{id}">>,
            description => <<"A document body.">>,
            mime_type => <<"application/json">>
        }},
        live_spec()
    ].

live_spec() ->
    {<<"barrel_live">>, live_resource, #{
        uri_template => <<"barrel://db/{db}/live/{sub}">>,
        description => <<"The materialized snapshot of a live query "
                         "(create one with query_subscribe; subscribe "
                         "to this URI for update notifications).">>,
        mime_type => <<"application/json">>
    }}.

%%====================================================================
%% Handlers
%%====================================================================

db_resource(#{<<"db">> := Name}) ->
    case barrel_server_dbs:ensure(Name) of
        {ok, Db} ->
            case barrel:info(Db) of
                {ok, Info} ->
                    barrel_server_http:jsonable(
                        barrel_server_http:encode_db_info(Info));
                {error, Reason} ->
                    #{error => err_bin(Reason)}
            end;
        {error, Reason} ->
            #{error => err_bin(Reason)}
    end.

doc_resource(#{<<"db">> := Name, <<"id">> := Id}) ->
    case barrel_server_dbs:ensure(Name) of
        {ok, Db} ->
            case barrel:get_doc(Db, Id) of
                {ok, Doc} -> Doc;
                {error, Reason} -> #{error => err_bin(Reason)}
            end;
        {error, Reason} ->
            #{error => err_bin(Reason)}
    end.

live_resource(#{<<"sub">> := SubId}) ->
    case barrel_server_mcp_live:snapshot(SubId) of
        {ok, Snapshot} -> Snapshot;
        {error, not_found} -> #{error => <<"not_found">>}
    end.

err_bin(Reason) when is_atom(Reason) ->
    atom_to_binary(Reason, utf8);
err_bin(Reason) ->
    iolist_to_binary(io_lib:format("~p", [Reason])).
