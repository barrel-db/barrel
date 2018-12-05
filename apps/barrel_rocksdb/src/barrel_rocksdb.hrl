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

-define(DB_OPEN_RETRIES, 30).
-define(DB_OPEN_RETRY_DELAY, 2000).

-define(rdb_store(Name), {n, l, {barrel_rocksdb_store, Name}}).
-define(rdb_cache(Name), {n, l, {barrel_rocksdb_cache, Name}}).