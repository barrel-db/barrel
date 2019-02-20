%% Copyright (c) 2018. Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%    http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.
-author("benoitc").


%% global keys (single byte)
-define(local_prefix, 16#01).
-define(local_max, 16#02).
-define(meta1_prefix, ?local_max).
-define(meta2_prefix, 16#03).
-define(meta_max, 16#04).
-define(system_prefix, ?meta_max).
-define(system_max, 16#05).

-define(key_min, <<"">>).

-define(key_max, << 16#ff, 16#ff >>).


-define(local_barrel_ident_prefix, << ?local_prefix, "db">>).


-define(docs_count_suffix, << "docs-count" >>).
-define(docs_del_count_suffix, << "docs-del-count" >>).
-define(purge_seq_suffix, << "purge-seq" >>).


-define(db_prefix, << ?system_prefix, "b" >>).

-define(docs_info_suffix, << "dinf" >>).
-define(docs_sec_suffix, << "dseq" >>).
-define(docs_revision_suffix, << "drev" >>).


-define(local_doc_prefix, <<"ld">>).


-define(view_key, <<"v">>).
-define(reverse_map_prefix, <<"r">>).
-define(index_prefix, <<"i">>).

