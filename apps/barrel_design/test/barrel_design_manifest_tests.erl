-module(barrel_design_manifest_tests).

-include_lib("eunit/include/eunit.hrl").

-define(DIGEST, <<1:256>>).
-define(SELECT, <<"SELECT * FROM db WHERE type = 'invoice' AND status = 'open'">>).

receivables() ->
    #{name => <<"receivables">>, version => 2,
      views => #{<<"open_by_customer">> =>
          #{select => ?SELECT,
            map => #{abi => 1, module => ?DIGEST, export => <<"map">>,
                     fields => [{[<<"customer">>], text}, {[<<"due_on">>], text},
                                {[<<"line_cents">>], {array, i64}}]},
            key => [#{type => text, category => true, title => <<"Customer">>},
                    #{type => text}],
            value => #{type => i64},
            reduce => '_sum'}}}.

native() ->
    #{<<"name">> => <<"receivables">>, <<"version">> => 1,
      <<"views">> => #{<<"open_by_customer">> =>
          #{<<"select">> => ?SELECT,
            <<"key">> => [#{<<"path">> => [<<"customer">>], <<"category">> => true,
                            <<"type">> => <<"text">>},
                          #{<<"path">> => [<<"due_on">>], <<"type">> => <<"text">>}],
            <<"value">> => #{<<"sum">> => [<<"line_cents">>]},
            <<"reduce">> => <<"_sum">>}}}.

normalize_wasm_test() ->
    {ok, #{name := <<"receivables">>, version := 2,
           views := #{<<"open_by_customer">> := V}}} =
        barrel_design_manifest:normalize(receivables()),
    ?assertMatch(#{map := #{abi := 1, module := ?DIGEST, export := <<"map">>},
                   key := {array, [#{type := text, category := true},
                                   #{type := text, category := false}]},
                   value := #{type := i64}, reduce := '_sum',
                   limits := #{fuel := 50000, timeout_ms := 100}}, V),
    ?assertEqual([?DIGEST], barrel_design_manifest:modules(
        element(2, barrel_design_manifest:normalize(receivables())))).

%% A JSON-shaped manifest (binary keys) normalizes like the atom form.
normalize_native_test() ->
    {ok, #{views := #{<<"open_by_customer">> := V}}} =
        barrel_design_manifest:normalize(native()),
    ?assertMatch(#{map := native, value := #{sum := [<<"line_cents">>], type := i64},
                   key := {array, [#{path := [<<"customer">>], category := true},
                                   #{path := [<<"due_on">>]}]}}, V).

hex_digest_test() ->
    D = receivables(),
    #{views := #{<<"open_by_customer">> := V0}} = D,
    Hex = binary:encode_hex(?DIGEST, lowercase),
    {ok, #{views := #{<<"open_by_customer">> := #{map := #{module := ?DIGEST}}}}} =
        barrel_design_manifest:normalize(
            D#{views := #{<<"open_by_customer">> =>
                              V0#{map := (maps:get(map, V0))#{module := Hex}}}}).

signature_test() ->
    Sig = fun(Design) ->
        {ok, #{views := #{<<"open_by_customer">> := V}}} = barrel_design_manifest:normalize(Design),
        barrel_design_manifest:signature(V)
    end,
    Base = receivables(),
    #{views := #{<<"open_by_customer">> := V0}} = Base,
    With = fun(V) -> Base#{views := #{<<"open_by_customer">> => V}} end,
    S0 = Sig(Base),
    ?assertEqual(32, byte_size(S0)),
    %% captions and the design version do not change the index
    ?assertEqual(S0, Sig(With(V0#{title => <<"Open invoices">>}))),
    ?assertEqual(S0, Sig(With(V0#{key := [#{type => text, category => true, title => <<"Client">>},
                                          #{type => text}]}))),
    ?assertEqual(S0, Sig(Base#{version := 3})),
    %% anything that changes rows does
    ?assertNotEqual(S0, Sig(With(V0#{select := <<"SELECT * FROM db WHERE type = 'invoice'">>}))),
    ?assertNotEqual(S0, Sig(With(V0#{map := (maps:get(map, V0))#{module := <<2:256>>}}))),
    ?assertNotEqual(S0, Sig(With(V0#{limits => #{fuel => 1000}}))),
    ?assertNotEqual(S0, Sig(With(V0#{reduce := '_count'}))).

rejections_test_() ->
    #{views := #{<<"open_by_customer">> := V}} = Base = receivables(),
    With = fun(View) -> Base#{views := #{<<"open_by_customer">> => View}} end,
    Bad = [
        {invalid_name, Base#{name := <<"Receivables">>}},
        {invalid_version, Base#{version := 0}},
        {invalid_views, Base#{views := #{}}},
        {unknown_key, Base#{extra => 1}},
        {invalid_select, With(V#{select := <<"SELECT * FROM db WHERE">>})},
        {invalid_select, With(V#{select := <<"SELECT id FROM db WHERE a = 1">>})},
        {invalid_select, With(V#{select := <<"SELECT * FROM db WHERE a = $x">>})},
        {invalid_select, With(V#{select := <<"SELECT * FROM db WHERE a = 1 ORDER BY a">>})},
        {invalid_select, With(V#{select := <<"SELECT * FROM vector_top_k('x') AS v">>})},
        {unsupported_abi, With(V#{map := (maps:get(map, V))#{abi => 2}})},
        {invalid_module_digest, With(V#{map := (maps:get(map, V))#{module => <<"xx">>}})},
        {invalid_field_type, With(V#{map := (maps:get(map, V))#{fields => [{[<<"a">>], float}]}})},
        {root_field_must_be_any, With(V#{map := (maps:get(map, V))#{fields => [{[], text}]}})},
        {category_not_leading, With(V#{key := [#{type => text}, #{type => text, category => true}]})},
        {wasm_key_has_path, With(V#{key := [#{type => text, path => [<<"a">>]}]})},
        {sum_needs_i64_value, With(V#{value := #{type => text}})},
        {invalid_reduce, With(V#{reduce := '_stats'})},
        {invalid_limit, With(V#{limits => #{fuel => 0}})},
        {invalid_limit, With(V#{limits => #{max_memory_pages => 17}})},
        {unknown_key, With(V#{limits => #{nope => 1}})},
        {native_key_needs_path, With(maps:remove(map, V))}
    ],
    [?_assertMatch({error, T} when is_tuple(T) andalso element(1, T) =:= Tag,
                   barrel_design_manifest:normalize(D)) || {Tag, D} <- Bad].

selector_test() ->
    {ok, S} = barrel_design_manifest:selector(?SELECT),
    ?assert(barrel_design_manifest:selected(S, #{<<"id">> => <<"a">>, <<"type">> => <<"invoice">>,
                                                 <<"status">> => <<"open">>})),
    ?assertNot(barrel_design_manifest:selected(S, #{<<"id">> => <<"a">>, <<"type">> => <<"invoice">>,
                                                    <<"status">> => <<"paid">>})),
    {ok, ById} = barrel_design_manifest:selector(
        <<"SELECT * FROM db WHERE id LIKE 'inv-%' AND status = 'open'">>),
    ?assert(barrel_design_manifest:selected(ById, #{<<"id">> => <<"inv-1">>, <<"status">> => <<"open">>})),
    ?assertNot(barrel_design_manifest:selected(ById, #{<<"id">> => <<"x-1">>, <<"status">> => <<"open">>})),
    ?assertNot(barrel_design_manifest:selected(ById, #{<<"id">> => <<"inv-1">>, <<"status">> => <<"paid">>})),
    {ok, All} = barrel_design_manifest:selector(<<"SELECT * FROM db">>),
    ?assert(barrel_design_manifest:selected(All, #{<<"id">> => <<"z">>})).
