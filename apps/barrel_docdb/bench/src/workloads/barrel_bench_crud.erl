%%%-------------------------------------------------------------------
%%% @doc CRUD workload for barrel_bench
%%%
%%% Benchmarks basic Create, Read, Update, Delete operations.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_bench_crud).

-export([run/3]).

%% @doc Run CRUD benchmarks
-spec run(pid(), non_neg_integer(), non_neg_integer()) -> map().
run(Db, NumDocs, Iterations) ->
    io:format("Running CRUD benchmarks...~n"),

    %% Phase 1: Sequential inserts
    io:format("  Inserting ~p documents...~n", [NumDocs]),
    InsertMetrics = bench_inserts(Db, NumDocs),

    %% Phase 2: Random reads
    io:format("  Reading ~p documents...~n", [Iterations]),
    ReadMetrics = bench_reads(Db, NumDocs, Iterations),

    %% Phase 3: Updates (read-modify-write)
    io:format("  Updating ~p documents...~n", [Iterations]),
    UpdateMetrics = bench_updates(Db, NumDocs, Iterations),

    %% Phase 4: Deletes (10% of docs)
    DeleteCount = max(1, NumDocs div 10),
    io:format("  Deleting ~p documents...~n", [DeleteCount]),
    DeleteMetrics = bench_deletes(Db, DeleteCount),

    #{
        insert => barrel_bench_metrics:summarize(InsertMetrics),
        read => barrel_bench_metrics:summarize(ReadMetrics),
        update => barrel_bench_metrics:summarize(UpdateMetrics),
        delete => barrel_bench_metrics:summarize(DeleteMetrics)
    }.

%%====================================================================
%% Internal functions
%%====================================================================

bench_inserts(Db, NumDocs) ->
    Metrics = barrel_bench_metrics:new(),
    lists:foldl(fun(I, Acc) ->
        Doc = barrel_bench_generator:user_doc(I),
        {Time, _Result} = timer:tc(fun() ->
            barrel_docdb:put_doc(Db, Doc)
        end),
        barrel_bench_metrics:record(Acc, Time)
    end, Metrics, lists:seq(0, NumDocs - 1)).

bench_reads(Db, NumDocs, Iterations) ->
    Metrics = barrel_bench_metrics:new(),
    lists:foldl(fun(I, Acc) ->
        %% Read random document
        DocIndex = I rem NumDocs,
        DocId = <<"user_", (integer_to_binary(DocIndex))/binary>>,
        {Time, _Result} = timer:tc(fun() ->
            barrel_docdb:get_doc(Db, DocId)
        end),
        barrel_bench_metrics:record(Acc, Time)
    end, Metrics, lists:seq(0, Iterations - 1)).

bench_updates(Db, NumDocs, Iterations) ->
    Metrics = barrel_bench_metrics:new(),
    lists:foldl(fun(I, Acc) ->
        %% Update random document
        DocIndex = I rem NumDocs,
        DocId = <<"user_", (integer_to_binary(DocIndex))/binary>>,
        {Time, _Result} = timer:tc(fun() ->
            case barrel_docdb:get_doc(Db, DocId) of
                {ok, Doc} ->
                    Updated = Doc#{<<"updated_at">> => I},
                    barrel_docdb:put_doc(Db, Updated);
                Error ->
                    Error
            end
        end),
        barrel_bench_metrics:record(Acc, Time)
    end, Metrics, lists:seq(0, Iterations - 1)).

bench_deletes(Db, Count) ->
    %% Delete from the end to avoid affecting read/update benchmarks
    Metrics = barrel_bench_metrics:new(),
    StartIndex = 1000000,  %% High index to avoid conflicts
    %% First insert docs to delete
    lists:foreach(fun(I) ->
        Doc = barrel_bench_generator:user_doc(StartIndex + I),
        barrel_docdb:put_doc(Db, Doc)
    end, lists:seq(0, Count - 1)),
    %% Now benchmark deletes
    lists:foldl(fun(I, Acc) ->
        DocId = <<"user_", (integer_to_binary(StartIndex + I))/binary>>,
        {Time, _Result} = timer:tc(fun() ->
            barrel_docdb:delete_doc(Db, DocId)
        end),
        barrel_bench_metrics:record(Acc, Time)
    end, Metrics, lists:seq(0, Count - 1)).
