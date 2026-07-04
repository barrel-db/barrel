%% Test transport stub: exports rep_id_term/1 so rep-id stability can
%% be asserted without a real network transport.
-module(barrel_rep_stub_transport).

-export([rep_id_term/1]).

rep_id_term(#{url := Url}) -> Url.
