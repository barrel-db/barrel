%% Copyright 2016-2017, Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.

-module(barrel_doc).

-export([
  is_deleted/1
]).

-export([
    revision_hash/3,
    revision_id/2,
    parse_revision/1,
    encode_revisions/1,
    parse_revisions/1,
    trim_history/3
  , compare/2
]).

-export([make_record/1]).



-export([id/1, rev/1, id_rev/1]).
-export([deleted/1]).

-type doc() :: #{ binary() => binary() }.
-type docid() :: binary().
-type revid() :: binary().

-export_types([doc/0, docid/0, revid/0]).

-include_lib("barrel/include/barrel.hrl").


is_deleted(Revtree) ->
  try
    barrel_revtree:fold_leafs(
      fun(RevInfo, _Acc) ->
        case barrel_revtree:is_deleted(RevInfo) of
          false  -> throw(deleted);
          true -> true
        end
      end,
      false,
      Revtree
    )
  catch
    throw:deleted -> false
  end.

%% @doc generate a unique revision hash for a document.
revision_hash(Doc, Rev, Deleted) ->
  Digest = ehash:hash(sha256, [Doc, Rev, Deleted]),
  barrel_lib:to_hex(Digest).


revision_id(RevPos, RevHash) when is_integer(RevPos), is_binary(RevHash) ->
  << (integer_to_binary(RevPos))/binary, "-", RevHash/binary >>.

parse_revision(<<"">>) -> {0, <<"">>};
parse_revision(Rev) when is_binary(Rev) ->
  case binary:split(Rev, <<"-">>) of
    [BinPos, Hash] -> {binary_to_integer(BinPos), Hash};
    _ -> exit({bad_rev, bad_format})
  end;
parse_revision(Rev) when is_list(Rev) -> parse_revision(list_to_binary(Rev));
parse_revision(_Rev) -> exit({bad_rev, bad_format}).

parse_revisions(#{ <<"revisions">> := Revisions}) ->
  case Revisions of
    #{ <<"start">> := Start, <<"ids">> := Ids} ->
      {Revs, _} = lists:foldl(
        fun(Id, {Acc, I}) ->
          Acc2 = [<< (integer_to_binary(I))/binary,"-", Id/binary >> | Acc],
          {Acc2, I - 1}
        end, {[], Start}, Ids),
      lists:reverse(Revs);
    _ -> []

  end;
parse_revisions(#{<<"_rev">> := Rev}) -> [Rev];
parse_revisions(_) -> [].

encode_revisions(Revs) ->
  [Oldest | _] = Revs,
  {Start, _} = barrel_doc:parse_revision(Oldest),
  Digests = lists:foldl(fun(Rev, Acc) ->
    {_, Digest}=parse_revision(Rev),
    [Digest | Acc]
                        end, [], Revs),
  #{<<"start">> => Start, <<"ids">> => lists:reverse(Digests)}.

trim_history(EncodedRevs, Ancestors, Limit) ->
  #{ <<"start">> := Start, <<"ids">> := Digests} = EncodedRevs,
  ADigests = array:from_list(Digests),
  {_, Limit2} = lists:foldl(
    fun(Ancestor, {Matched, Unmatched}) ->
      {Gen, Digest} = barrel_doc:parse_revision(Ancestor),
      Idx = Start - Gen,
      IsDigest = array:get(Idx, ADigests) =:= Digest,
      if
        Idx >= 0, Idx < Matched, IsDigest =:= true-> {Idx, Idx+1};
        true -> {Matched, Unmatched}
      end
    end, {length(Digests), Limit}, Ancestors),
  EncodedRevs#{ <<"ids">> => (lists:sublist(Digests, Limit2)) }.

compare(RevA, RevB) ->
  RevTupleA = parse_revision(RevA),
  RevTupleB = parse_revision(RevB),
  compare1(RevTupleA, RevTupleB).

compare1(RevA, RevB) when RevA > RevB -> 1;
compare1(RevA, RevB) when RevA < RevB -> -1;
compare1(_, _) -> 0.

-spec id(doc()) -> docid() | undefined.
id(#{<<"id">> := Id}) -> Id;
id(#{}) -> undefined;
id(_) -> erlang:error(bad_doc).

rev(#{<<"_rev">> := Rev}) -> Rev;
rev(#{}) -> <<>>;
rev(_) -> error(bad_doc).

-spec id_rev(doc()) -> {docid(), revid()}.
id_rev(#{<<"id">> := Id, <<"_rev">> := Rev}) -> {Id, Rev};
id_rev(#{<<"id">> := Id}) -> {Id, <<>>};
id_rev(#{<<"_rev">> := _Rev}) -> erlang:error(bad_doc);
id_rev(#{}) -> {undefined, <<>>};
id_rev(_) -> erlang:error(bad_doc).

deleted(#{ <<"_deleted">> := Del}) when is_boolean(Del) -> Del;
deleted(_) -> false.

doc_without_meta(Doc) ->
  maps:filter(
    fun
      (<< "_attachments", _/binary >>, _) -> true;
      (<< "_", _/binary >>, _) -> false;
      (_, _) -> true
    end,
    Doc
  ).

make_record(#{ <<"id">> := Id, <<"doc">> := Doc0, <<"history">> := History }) ->
  Deleted = maps:get(<<"deleted">>, Doc0, false),
  Doc1 = doc_without_meta(Doc0),
  #{id => Id,
    ref => erlang:make_ref(),
    revs => History,
    deleted => Deleted,
    doc => Doc1};
make_record(Doc0) ->
  Deleted = maps:get(<<"_deleted">>, Doc0, false),
  Rev = maps:get(<<"_rev">>, Doc0, <<"">>),
  Id = case maps:find(<<"id">>, Doc0) of
         {ok, DocId} -> DocId;
         error ->
           barrel_lib:uniqid()
       end,
  Doc1 = doc_without_meta(Doc0),
  Hash = revision_hash(Doc1, Rev, Deleted),
  Revs = case Rev of
           <<"">> -> [<< "1-", Hash/binary>> ];
           _ ->
             {Gen, _}  = barrel_doc:parse_revision(Rev),
             [<< (integer_to_binary(Gen +1))/binary, "-", Hash/binary >>, Rev]
         end,
  #{id => Id,
    ref => erlang:make_ref(),
    revs => Revs,
    deleted => Deleted,
    hash => Hash,
    doc => Doc1}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

parse_test() ->
  Parsed = parse_revision(<<"10-2f25ea96da3fed514795b0ced028d58a">>),
  ?assertEqual({10, <<"2f25ea96da3fed514795b0ced028d58a">>}, Parsed).

compare_test() ->
  ?assertEqual(0, compare(<<"1-a">>, <<"1-a">>)),
  ?assertEqual(1, compare(<<"1-b">>, <<"1-a">>)),
  ?assertEqual(-1, compare(<<"1-a">>, <<"1-b">>)),
  ?assertEqual(1, compare(<<"2-a">>, <<"1-a">>)),
  ?assertEqual(1, compare(<<"2-b">>, <<"1-a">>)).

deleted_test() ->
  ?assertEqual(true, deleted(#{ <<"_deleted">> => true})),
  ?assertEqual(false, deleted(#{ <<"_deleted">> => false})).

encode_revisions_test() ->
  Revs = [<<"2-b19b17d048f082aa4a62c8da1262a33a">>,<<"1-5c1f0a9d721f0731a46645d18b763047">>],
  ?assertEqual(#{
                 <<"start">> => 2,
                 <<"ids">> => [<<"b19b17d048f082aa4a62c8da1262a33a">>, <<"5c1f0a9d721f0731a46645d18b763047">>]
               }, encode_revisions(Revs)).

parse_revisions_test() ->
  Revs = [<<"2-b19b17d048f082aa4a62c8da1262a33a">>,<<"1-5c1f0a9d721f0731a46645d18b763047">>],
  Body = #{
           <<"revisions">> => #{
             <<"start">> => 2,
             <<"ids">> => [<<"b19b17d048f082aa4a62c8da1262a33a">>, <<"5c1f0a9d721f0731a46645d18b763047">>]
           }
         },
  ?assertEqual(Revs, parse_revisions(Body)).



-endif.
