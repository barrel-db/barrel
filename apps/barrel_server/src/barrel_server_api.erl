%%%-------------------------------------------------------------------
%%% @doc Embeddable route table for the barrel REST/sync surface.
%%%
%%% Exposes barrel's routes as a plain livery route list (and a compiled
%%% router) so a host livery application can mount them into its own
%%% service, optionally under a sub-path, and apply its own middleware.
%%% The route handlers live in {@link barrel_server_http},
%%% {@link barrel_server_sync}, {@link barrel_server_spaces}, and
%%% {@link barrel_server_mcp}; this module only assembles the table.
%%%
%%% Routes are grouped so an embedder can take just what it needs:
%%% <ul>
%%%   <li>`meta'     - `/' and `/health' (standalone only by default).</li>
%%%   <li>`db'       - database lifecycle, documents, bulk, find, query,
%%%                    changes, attachments, vector add.</li>
%%%   <li>`sync'     - the replication wire (`/db/:db/_sync/*').</li>
%%%   <li>`timeline' - branch, merge, lineage.</li>
%%%   <li>`search'   - vector/bm25/hybrid search.</li>
%%%   <li>`spaces'   - the agent layer (`/spaces', `/handoffs').</li>
%%%   <li>`mcp'      - the MCP endpoint (empty when disabled).</li>
%%% </ul>
%%%
%%% `routes/0' returns the default DB surface (`db', `sync', `timeline',
%%% `search') that barrel-lite needs. The standalone service
%%% ({@link barrel_server_http}) takes `groups => all'.
%%%
%%% Embedding (DB surface, host owns auth) needs no barrel_server
%%% supervisor; just start `barrel' and `barrel_spaces', then:
%%% ```
%%% Barrel  = barrel_server_api:router(),
%%% HostR1  = livery_router:nest(<<"/barrel">>, Barrel, HostRouter),
%%% {ok, _} = livery:start_service(#{http => #{port => 8080},
%%%                                  router => HostR1,
%%%                                  middleware => HostAuthStack}).
%%% '''
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_api).

-export([routes/0, routes/1, router/0, router/1, groups/0]).

-type group() :: meta | db | sync | timeline | search | spaces | mcp.
-type route() :: {binary(), binary(), term()}.
-export_type([group/0, route/0]).

%% Groups served in an embedder's default (what barrel-lite needs).
-define(DEFAULT_GROUPS, [db, sync, timeline, search]).
%% Every group, in a stable order. `meta' first so `/' and `/health'
%% keep working in the standalone service.
-define(ALL_GROUPS, [meta, db, timeline, sync, search, spaces, mcp]).

%%====================================================================
%% API
%%====================================================================

%% @doc The known route groups.
-spec groups() -> [group()].
groups() ->
    ?ALL_GROUPS.

%% @doc The default DB-surface routes (`db', `sync', `timeline', `search').
-spec routes() -> [route()].
routes() ->
    routes(#{groups => ?DEFAULT_GROUPS}).

%% @doc Routes for the selected groups. `Opts' is `#{groups => all |
%% [group()]}'; unknown group names fail loud.
-spec routes(map()) -> [route()].
routes(Opts) when is_map(Opts) ->
    Groups = resolve_groups(maps:get(groups, Opts, ?DEFAULT_GROUPS)),
    lists:append([group_routes(G) || G <- Groups]).

%% @doc A compiled router for the default DB surface.
-spec router() -> livery_router:router().
router() ->
    router(#{}).

%% @doc A compiled router for the selected groups. In addition to
%% `groups', `Opts' may carry `prefix => binary()' to mount the routes
%% under a sub-path (equivalent to `livery_router:nest/2').
-spec router(map()) -> livery_router:router().
router(Opts) when is_map(Opts) ->
    Router = livery_router:compile(routes(Opts)),
    case maps:get(prefix, Opts, undefined) of
        undefined -> Router;
        <<>> -> Router;
        Prefix when is_binary(Prefix) -> livery_router:nest(Prefix, Router)
    end.

%%====================================================================
%% Internal
%%====================================================================

resolve_groups(all) ->
    ?ALL_GROUPS;
resolve_groups(Groups) when is_list(Groups) ->
    lists:foreach(fun validate_group/1, Groups),
    Groups.

validate_group(Group) ->
    case lists:member(Group, ?ALL_GROUPS) of
        true -> ok;
        false -> error({unknown_route_group, Group})
    end.

-spec group_routes(group()) -> [route()].
group_routes(meta) ->
    [
        {<<"GET">>, <<"/">>,       {barrel_server_http, root}},
        {<<"GET">>, <<"/health">>, {barrel_server_http, health}}
    ];
group_routes(db) ->
    [
        {<<"PUT">>,    <<"/db/:db">>,                        {barrel_server_http, create_db}},
        {<<"GET">>,    <<"/db/:db">>,                        {barrel_server_http, db_info}},
        {<<"DELETE">>, <<"/db/:db">>,                        {barrel_server_http, drop_db}},

        {<<"PUT">>,    <<"/db/:db/doc/:id">>,               {barrel_server_http, put_doc}},
        {<<"GET">>,    <<"/db/:db/doc/:id">>,               {barrel_server_http, get_doc}},
        {<<"DELETE">>, <<"/db/:db/doc/:id">>,               {barrel_server_http, delete_doc}},
        {<<"GET">>,    <<"/db/:db/_history">>,              {barrel_server_http, history}},
        {<<"GET">>,    <<"/db/:db/doc/:id/_versions">>,     {barrel_server_http, doc_versions}},
        {<<"GET">>,    <<"/db/:db/doc/:id/_versions/:rev">>, {barrel_server_http, version_body}},
        {<<"POST">>,   <<"/db/:db/_bulk_docs">>,            {barrel_server_http, bulk_docs}},
        {<<"POST">>,   <<"/db/:db/_bulk_get">>,             {barrel_server_http, bulk_get}},
        {<<"POST">>,   <<"/db/:db/find">>,                  {barrel_server_http, find}},
        %% GET exists for browser EventSource (SUBSCRIBE over SSE)
        {<<"POST">>,   <<"/db/:db/query">>,                 {barrel_server_http, query}},
        {<<"GET">>,    <<"/db/:db/query">>,                 {barrel_server_http, query}},
        {<<"GET">>,    <<"/db/:db/changes">>,               {barrel_server_http, changes}},

        {<<"PUT">>,    <<"/db/:db/doc/:id/att/:name">>,     {barrel_server_http, put_att}},
        {<<"GET">>,    <<"/db/:db/doc/:id/att/:name">>,     {barrel_server_http, get_att}},
        {<<"DELETE">>, <<"/db/:db/doc/:id/att/:name">>,     {barrel_server_http, delete_att}},

        {<<"POST">>,   <<"/db/:db/vector">>,                {barrel_server_http, vector_add}}
    ];
group_routes(timeline) ->
    [
        {<<"GET">>,  <<"/db/:db/_timeline">>,        {barrel_server_http, timeline_info}},
        {<<"POST">>, <<"/db/:db/_timeline/branch">>, {barrel_server_http, timeline_branch}},
        {<<"POST">>, <<"/db/:db/_timeline/merge">>,  {barrel_server_http, timeline_merge}}
    ];
group_routes(sync) ->
    [
        {<<"GET">>,    <<"/db/:db/_sync/info">>,          {barrel_server_sync, info}},
        {<<"POST">>,   <<"/db/:db/_sync/hlc">>,           {barrel_server_sync, sync_hlc}},
        {<<"POST">>,   <<"/db/:db/_sync/changes">>,       {barrel_server_sync, changes}},
        {<<"POST">>,   <<"/db/:db/_sync/diff">>,          {barrel_server_sync, diff}},
        {<<"GET">>,    <<"/db/:db/_sync/doc/:id">>,       {barrel_server_sync, get_doc}},
        {<<"PUT">>,    <<"/db/:db/_sync/doc/:id">>,       {barrel_server_sync, put_version}},
        {<<"GET">>,    <<"/db/:db/_sync/local/:id">>,     {barrel_server_sync, get_local}},
        {<<"PUT">>,    <<"/db/:db/_sync/local/:id">>,     {barrel_server_sync, put_local}},
        {<<"DELETE">>, <<"/db/:db/_sync/local/:id">>,     {barrel_server_sync, delete_local}},
        {<<"GET">>,    <<"/db/:db/_sync/att_changes">>,   {barrel_server_sync, att_changes}},
        {<<"POST">>,   <<"/db/:db/_sync/att_diff">>,      {barrel_server_sync, att_diff}},
        {<<"GET">>,    <<"/db/:db/_sync/att/:id/:name">>, {barrel_server_sync, get_att}},
        {<<"PUT">>,    <<"/db/:db/_sync/att/:id/:name">>, {barrel_server_sync, put_att}},
        {<<"DELETE">>, <<"/db/:db/_sync/att/:id/:name">>, {barrel_server_sync, delete_att}}
    ];
group_routes(search) ->
    [
        {<<"POST">>, <<"/db/:db/search/vector">>, {barrel_server_http, search_vector}},
        {<<"POST">>, <<"/db/:db/search/bm25">>,   {barrel_server_http, search_bm25}},
        {<<"POST">>, <<"/db/:db/search/hybrid">>, {barrel_server_http, search_hybrid}}
    ];
group_routes(spaces) ->
    barrel_server_spaces:routes();
group_routes(mcp) ->
    barrel_server_mcp:routes().
