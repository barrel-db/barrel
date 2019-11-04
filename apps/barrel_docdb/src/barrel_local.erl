-module(barrel_local).

-export([put_doc/3,
         delete_doc/2,
         get_doc/2]).

-include_lib("barrel/include/barrel.hrl").

put_doc(#{ ref := Ref }, DocId, Doc) ->
  ?STORE:put_local_doc(Ref, DocId, Doc).

delete_doc(#{ ref := Ref }, DocId) ->
  ?STORE:delete_local_doc(Ref, DocId).

get_doc(#{ ref := Ref }, DocId) ->
   ?STORE:get_local_doc(Ref, DocId).
