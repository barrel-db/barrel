-module(barrel_metrics).

-export([init/0]).

-define(METRICS,
        [{'barrel/db/fetch_doc_duration', "duration to fetch a doc in millisecond", 'ms'},
         {'barrel/db/fetch_doc_num', "docs fetched",'1'},
         {'barrel/db/update_doc_num', "docs updated", '1'},
         {'barrel/db/update_doc_duration', "doc update duration", 'ms'},
         {'barrel/db/update_doc_timeout', "number of doc update timeout", '1'},
         {'barrel/db/fold_docs_num', "number of doc folds", '1'},
         {'barrel/docs/fold_docs_duration', "duration when traversing docs", 'ms'},
         {'barrel/db/fold_changes_num', "number of fold changes", '1'},
         {'barrel/db/fold_change_duration', "duration when traversing changes", 'ms'},
         {'barrel/dbs/active_num', "docs updated", '1'},
         {'barrel/views/fold_count', "number of view traversing processes", '1'},
         {'barrel/views/fold_duration', "duration when traversing a view", 'ms'},
         {'barrel/views/active_num', "number of active views", '1'},
         {'barrel/views/active_workers', "number of active workers indexing documents", '1'},
         {'barrel/views/index_duration', "duration while processing a document", 'ms'},
         {'barrel/views/docs_indexed', "number of docs indexed", '1'},
         {'barrel/attachments/put_num', "number of attachments put", '1'},
         {'barrel/attachments/put_duration', "duration when putting an attachment", 'ms'},
         {'barrel/attachments/fetch_num', "number of attachments fetched", '1'},
         {'barrel/attachments/fetch_duration', "duration when fetching an attachment", 'ms'},
         {'barrel/attachments/active', "number of active attachments", '1'},


         %% rate keeper
         {'barrel/storage/get_latency', "latency of get", 'ms'},
         {'barrel/storage/put_latency', "latency of put", 'ms'}

        ]).


init() ->
  lists:foreach(fun({N, D, U}) ->
                    oc_stat_measure:new(N, D, U)
                end, ?METRICS),
  ok.
