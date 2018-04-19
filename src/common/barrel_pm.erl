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

-module(barrel_pm).
-author("benoitc").

%% API
-export([
  whereis_name/1,
  register_name/2,
  unregister_name/1
]).

-include("barrel.hrl").


whereis_name({Name, Node}) ->
  rpc:call(Node, gproc, whereis_name, [?barrel(Name)]);
whereis_name(Name) ->
  gproc:whereis_name(?barrel(Name)).

register_name(Name, Pid) ->
  gproc:register_name(?barrel(Name), Pid).

unregister_name(Name) ->
  gproc:unregister_name(?barrel(Name)).



