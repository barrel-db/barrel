-module(barrel_db_attachments).

-export([put_attachment/4,
         fetch_attachment/4]).

-include("barrel.hrl").

put_attachment(BarrelId, DocId, AttName, Contents) ->
  {AttBin, Headers} = maybe_encode_att(AttName, Contents),
  case ?STORE:put_attachment(BarrelId, DocId, AttName, AttBin) of
    {ok, Att} ->
      {ok, #{ attachment => Att,
              headers => Headers }};
    Error ->
      Error
  end.

fetch_attachment(Ctx, DocId, AttName, Att) ->
  ?STORE:fetch_attachment(Ctx, DocId, AttName, Att).


maybe_encode_att(AttName, Contents) when is_binary(Contents) ->
  Headers = #{ << "Content-Type" >> => mimerl:filename(AttName),
               << "Content-Length" >> => byte_size(Contents) },
  {Contents, Headers};

maybe_encode_att(_AttName, Contents) ->
  Bin = term_to_binary(Contents),
  Headers = #{ << "Content-Type" >> => << "application/erlang-term" >> ,
               << "Content-Length" >> => byte_size(Bin) },
  {Bin, Headers}.
