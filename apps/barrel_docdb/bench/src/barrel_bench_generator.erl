%%%-------------------------------------------------------------------
%%% @doc Document generator for barrel_bench
%%%
%%% Generates documents for benchmarking with configurable patterns
%%% that support different query types, sizes, and structures.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_bench_generator).

-export([user_doc/1, user_doc/2]).
-export([random_doc/1, random_doc/2]).
-export([batch/3]).

%% Document size/structure generators
-export([
    small_flat_doc/1,
    medium_flat_doc/1,
    large_flat_doc/1,
    small_nested_doc/1,
    medium_nested_doc/1,
    large_nested_doc/1
]).

%% Generator info
-export([doc_types/0, doc_type_info/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type doc_options() :: #{
    id => binary(),
    age_range => {non_neg_integer(), non_neg_integer()},
    cities => [binary()],
    statuses => [binary()]
}.

-export_type([doc_options/0]).

%% Cities for random selection
-define(DEFAULT_CITIES, [
    <<"Paris">>, <<"London">>, <<"Berlin">>, <<"Madrid">>,
    <<"Rome">>, <<"Amsterdam">>, <<"Vienna">>, <<"Prague">>
]).

%% Statuses for random selection
-define(DEFAULT_STATUSES, [
    <<"active">>, <<"inactive">>, <<"pending">>
]).

%% Categories for documents
-define(CATEGORIES, [
    <<"electronics">>, <<"clothing">>, <<"books">>, <<"home">>,
    <<"sports">>, <<"toys">>, <<"food">>, <<"health">>
]).

%%====================================================================
%% Document Type Registry
%%====================================================================

%% @doc List all available document types for benchmarking
-spec doc_types() -> [atom()].
doc_types() ->
    [small_flat, medium_flat, large_flat,
     small_nested, medium_nested, large_nested].

%% @doc Get info about a document type
-spec doc_type_info(atom()) -> map().
doc_type_info(small_flat) ->
    #{name => <<"Small Flat">>, fields => 5, depth => 1,
      approx_size => <<"~200 bytes">>, generator => fun small_flat_doc/1};
doc_type_info(medium_flat) ->
    #{name => <<"Medium Flat">>, fields => 20, depth => 1,
      approx_size => <<"~1 KB">>, generator => fun medium_flat_doc/1};
doc_type_info(large_flat) ->
    #{name => <<"Large Flat">>, fields => 100, depth => 1,
      approx_size => <<"~5 KB">>, generator => fun large_flat_doc/1};
doc_type_info(small_nested) ->
    #{name => <<"Small Nested">>, fields => 8, depth => 3,
      approx_size => <<"~300 bytes">>, generator => fun small_nested_doc/1};
doc_type_info(medium_nested) ->
    #{name => <<"Medium Nested">>, fields => 25, depth => 4,
      approx_size => <<"~2 KB">>, generator => fun medium_nested_doc/1};
doc_type_info(large_nested) ->
    #{name => <<"Large Nested">>, fields => 80, depth => 5,
      approx_size => <<"~8 KB">>, generator => fun large_nested_doc/1}.

%%====================================================================
%% Small Flat Document (~5 fields, ~200 bytes)
%%====================================================================

%% @doc Generate a small flat document - minimal fields, no nesting
-spec small_flat_doc(non_neg_integer()) -> map().
small_flat_doc(Index) ->
    #{
        <<"_id">> => make_id(<<"sf">>, Index),
        <<"type">> => <<"item">>,
        <<"name">> => <<"Item ", (integer_to_binary(Index))/binary>>,
        <<"status">> => pick(?DEFAULT_STATUSES, Index),
        <<"value">> => Index rem 1000
    }.

%%====================================================================
%% Medium Flat Document (~20 fields, ~1KB)
%%====================================================================

%% @doc Generate a medium flat document - more fields, still flat
-spec medium_flat_doc(non_neg_integer()) -> map().
medium_flat_doc(Index) ->
    Status = pick(?DEFAULT_STATUSES, Index),
    Category = pick(?CATEGORIES, Index),
    City = pick(?DEFAULT_CITIES, Index),
    #{
        <<"_id">> => make_id(<<"mf">>, Index),
        <<"type">> => <<"product">>,
        <<"name">> => <<"Product ", (integer_to_binary(Index))/binary>>,
        <<"description">> => generate_text(50 + (Index rem 50)),
        <<"status">> => Status,
        <<"category">> => Category,
        <<"price">> => (Index rem 10000) / 100,
        <<"quantity">> => Index rem 500,
        <<"sku">> => <<"SKU-", (integer_to_binary(100000 + Index))/binary>>,
        <<"brand">> => <<"Brand ", (integer_to_binary(Index rem 20))/binary>>,
        <<"weight">> => (Index rem 1000) / 10,
        <<"color">> => pick([<<"red">>, <<"blue">>, <<"green">>, <<"black">>, <<"white">>], Index),
        <<"size">> => pick([<<"S">>, <<"M">>, <<"L">>, <<"XL">>], Index),
        <<"rating">> => (Index rem 50) / 10,
        <<"reviews_count">> => Index rem 1000,
        <<"in_stock">> => (Index rem 3) =/= 0,
        <<"featured">> => (Index rem 10) =:= 0,
        <<"city">> => City,
        <<"country">> => city_country(City),
        <<"created_at">> => Index,
        <<"updated_at">> => Index + 1000
    }.

%%====================================================================
%% Large Flat Document (~100 fields, ~5KB)
%%====================================================================

%% @doc Generate a large flat document - many fields, no nesting
-spec large_flat_doc(non_neg_integer()) -> map().
large_flat_doc(Index) ->
    Base = medium_flat_doc(Index),
    %% Add 80 more fields
    Extra = maps:from_list([
        {iolist_to_binary([<<"field_">>, integer_to_binary(I)]),
         generate_field_value(I, Index)}
        || I <- lists:seq(1, 80)
    ]),
    maps:merge(Base#{<<"_id">> => make_id(<<"lf">>, Index)}, Extra).

%%====================================================================
%% Small Nested Document (~8 fields, 3 levels deep, ~300 bytes)
%%====================================================================

%% @doc Generate a small nested document - few fields, moderate nesting
-spec small_nested_doc(non_neg_integer()) -> map().
small_nested_doc(Index) ->
    #{
        <<"_id">> => make_id(<<"sn">>, Index),
        <<"type">> => <<"order">>,
        <<"status">> => pick(?DEFAULT_STATUSES, Index),
        <<"customer">> => #{
            <<"name">> => <<"Customer ", (integer_to_binary(Index rem 100))/binary>>,
            <<"email">> => <<"user", (integer_to_binary(Index))/binary, "@test.com">>
        },
        <<"shipping">> => #{
            <<"city">> => pick(?DEFAULT_CITIES, Index),
            <<"priority">> => pick([<<"standard">>, <<"express">>, <<"overnight">>], Index)
        }
    }.

%%====================================================================
%% Medium Nested Document (~25 fields, 4 levels deep, ~2KB)
%%====================================================================

%% @doc Generate a medium nested document - moderate fields and nesting
-spec medium_nested_doc(non_neg_integer()) -> map().
medium_nested_doc(Index) ->
    City = pick(?DEFAULT_CITIES, Index),
    #{
        <<"_id">> => make_id(<<"mn">>, Index),
        <<"type">> => <<"account">>,
        <<"status">> => pick(?DEFAULT_STATUSES, Index),
        <<"created_at">> => Index,
        <<"profile">> => #{
            <<"name">> => <<"User ", (integer_to_binary(Index))/binary>>,
            <<"age">> => 18 + (Index rem 62),
            <<"bio">> => generate_text(30),
            <<"contact">> => #{
                <<"email">> => <<"user", (integer_to_binary(Index))/binary, "@example.com">>,
                <<"phone">> => <<"+1-555-", (integer_to_binary(1000000 + Index))/binary>>,
                <<"verified">> => (Index rem 2) =:= 0
            },
            <<"location">> => #{
                <<"city">> => City,
                <<"country">> => city_country(City),
                <<"coordinates">> => #{
                    <<"lat">> => 40.0 + (Index rem 100) / 10,
                    <<"lon">> => -74.0 + (Index rem 100) / 10
                }
            }
        },
        <<"preferences">> => #{
            <<"theme">> => pick([<<"light">>, <<"dark">>, <<"auto">>], Index),
            <<"language">> => pick([<<"en">>, <<"fr">>, <<"de">>, <<"es">>], Index),
            <<"notifications">> => #{
                <<"email">> => (Index rem 2) =:= 0,
                <<"push">> => (Index rem 3) =:= 0,
                <<"sms">> => (Index rem 5) =:= 0
            }
        },
        <<"stats">> => #{
            <<"logins">> => Index rem 1000,
            <<"posts">> => Index rem 500,
            <<"followers">> => Index rem 10000
        }
    }.

%%====================================================================
%% Large Nested Document (~80 fields, 5 levels deep, ~8KB)
%%====================================================================

%% @doc Generate a large nested document - many fields with deep nesting
-spec large_nested_doc(non_neg_integer()) -> map().
large_nested_doc(Index) ->
    City = pick(?DEFAULT_CITIES, Index),
    Category = pick(?CATEGORIES, Index),
    #{
        <<"_id">> => make_id(<<"ln">>, Index),
        <<"type">> => <<"organization">>,
        <<"status">> => pick(?DEFAULT_STATUSES, Index),
        <<"created_at">> => Index,
        <<"updated_at">> => Index + 1000,
        <<"metadata">> => #{
            <<"version">> => 1,
            <<"schema">> => <<"v2">>,
            <<"tags">> => generate_tags(Index)
        },
        <<"company">> => #{
            <<"name">> => <<"Company ", (integer_to_binary(Index))/binary>>,
            <<"industry">> => Category,
            <<"size">> => pick([<<"startup">>, <<"small">>, <<"medium">>, <<"enterprise">>], Index),
            <<"founded">> => 1990 + (Index rem 34),
            <<"headquarters">> => #{
                <<"address">> => #{
                    <<"street">> => <<"123 Main St">>,
                    <<"city">> => City,
                    <<"country">> => city_country(City),
                    <<"postal">> => integer_to_binary(10000 + Index rem 90000)
                },
                <<"contact">> => #{
                    <<"phone">> => <<"+1-555-", (integer_to_binary(1000000 + Index))/binary>>,
                    <<"email">> => <<"contact@company", (integer_to_binary(Index))/binary, ".com">>,
                    <<"website">> => <<"https://company", (integer_to_binary(Index))/binary, ".com">>
                }
            },
            <<"departments">> => #{
                <<"engineering">> => generate_department(Index, <<"engineering">>),
                <<"sales">> => generate_department(Index + 1, <<"sales">>),
                <<"marketing">> => generate_department(Index + 2, <<"marketing">>),
                <<"hr">> => generate_department(Index + 3, <<"hr">>)
            }
        },
        <<"financials">> => #{
            <<"revenue">> => #{
                <<"current">> => Index * 10000,
                <<"previous">> => Index * 9000,
                <<"growth">> => 11.1
            },
            <<"expenses">> => #{
                <<"operations">> => Index * 3000,
                <<"marketing">> => Index * 2000,
                <<"rd">> => Index * 4000
            },
            <<"metrics">> => #{
                <<"arr">> => Index * 120000,
                <<"mrr">> => Index * 10000,
                <<"churn">> => 2.5,
                <<"ltv">> => Index * 500
            }
        },
        <<"products">> => [
            generate_product(Index, I) || I <- lists:seq(1, 3)
        ],
        <<"extra_data">> => maps:from_list([
            {iolist_to_binary([<<"extra_">>, integer_to_binary(I)]),
             generate_field_value(I, Index)}
            || I <- lists:seq(1, 20)
        ])
    }.

%%====================================================================
%% Legacy user_doc (kept for backwards compatibility)
%%====================================================================

%% @doc Generate a user document with auto-generated ID
-spec user_doc(non_neg_integer()) -> map().
user_doc(Index) ->
    user_doc(Index, #{}).

%% @doc Generate a user document with options
-spec user_doc(non_neg_integer(), doc_options()) -> map().
user_doc(Index, Opts) ->
    Id = maps:get(id, Opts, make_id(<<"user">>, Index)),
    {MinAge, MaxAge} = maps:get(age_range, Opts, {18, 80}),
    Cities = maps:get(cities, Opts, ?DEFAULT_CITIES),
    Statuses = maps:get(statuses, Opts, ?DEFAULT_STATUSES),

    Age = MinAge + (Index rem (MaxAge - MinAge + 1)),
    City = pick(Cities, Index),
    Status = pick(Statuses, Index),

    #{
        <<"_id">> => Id,
        <<"type">> => <<"user">>,
        <<"name">> => <<"User ", (integer_to_binary(Index))/binary>>,
        <<"age">> => Age,
        <<"status">> => Status,
        <<"profile">> => #{
            <<"city">> => City,
            <<"country">> => city_country(City)
        },
        <<"tags">> => generate_tags(Index),
        <<"created_at">> => Index
    }.

%% @doc Generate a random document with auto-generated ID
-spec random_doc(non_neg_integer()) -> map().
random_doc(Index) ->
    random_doc(Index, #{}).

%% @doc Generate a random document with configurable payload size
-spec random_doc(non_neg_integer(), doc_options()) -> map().
random_doc(Index, Opts) ->
    Id = maps:get(id, Opts, make_id(<<"doc">>, Index)),
    #{
        <<"_id">> => Id,
        <<"type">> => <<"generic">>,
        <<"index">> => Index,
        <<"data">> => generate_payload(Index)
    }.

%% @doc Generate a batch of documents
-spec batch(fun((non_neg_integer()) -> map()), non_neg_integer(), non_neg_integer()) -> [map()].
batch(Generator, Start, Count) ->
    [Generator(I) || I <- lists:seq(Start, Start + Count - 1)].

%%====================================================================
%% Internal functions
%%====================================================================

make_id(Prefix, Index) ->
    <<Prefix/binary, "_", (integer_to_binary(Index))/binary>>.

pick(List, Index) ->
    lists:nth((Index rem length(List)) + 1, List).

city_country(<<"Paris">>) -> <<"France">>;
city_country(<<"London">>) -> <<"UK">>;
city_country(<<"Berlin">>) -> <<"Germany">>;
city_country(<<"Madrid">>) -> <<"Spain">>;
city_country(<<"Rome">>) -> <<"Italy">>;
city_country(<<"Amsterdam">>) -> <<"Netherlands">>;
city_country(<<"Vienna">>) -> <<"Austria">>;
city_country(<<"Prague">>) -> <<"Czech Republic">>;
city_country(_) -> <<"Unknown">>.

generate_tags(Index) ->
    Tags = [<<"tag1">>, <<"tag2">>, <<"tag3">>, <<"tag4">>],
    NumTags = (Index rem 3) + 1,
    lists:sublist(Tags, NumTags).

generate_payload(Index) ->
    %% Generate a simple payload that varies by index
    base64:encode(crypto:strong_rand_bytes(64 + (Index rem 64))).

generate_text(Length) ->
    Words = [<<"lorem">>, <<"ipsum">>, <<"dolor">>, <<"sit">>, <<"amet">>,
             <<"consectetur">>, <<"adipiscing">>, <<"elit">>, <<"sed">>, <<"do">>],
    generate_text_loop(Words, Length, []).

generate_text_loop(_Words, Length, Acc) when Length =< 0 ->
    iolist_to_binary(lists:reverse(Acc));
generate_text_loop(Words, Length, Acc) ->
    Word = lists:nth((Length rem length(Words)) + 1, Words),
    WordLen = byte_size(Word),
    Sep = case Acc of [] -> <<>>; _ -> <<" ">> end,
    generate_text_loop(Words, Length - WordLen - 1, [Word, Sep | Acc]).

generate_field_value(FieldNum, Index) ->
    case FieldNum rem 5 of
        0 -> Index + FieldNum;
        1 -> (Index + FieldNum) / 100;
        2 -> <<"value_", (integer_to_binary(Index + FieldNum))/binary>>;
        3 -> (Index + FieldNum) rem 2 =:= 0;
        4 -> [FieldNum, Index rem 100, <<"item">>]
    end.

generate_department(Index, Name) ->
    #{
        <<"name">> => Name,
        <<"head_count">> => 10 + (Index rem 100),
        <<"budget">> => Index * 1000,
        <<"manager">> => <<"Manager ", (integer_to_binary(Index rem 50))/binary>>
    }.

generate_product(Index, Num) ->
    #{
        <<"id">> => <<"prod_", (integer_to_binary(Index * 10 + Num))/binary>>,
        <<"name">> => <<"Product ", (integer_to_binary(Num))/binary>>,
        <<"price">> => (Index + Num) * 10,
        <<"active">> => (Index + Num) rem 2 =:= 0
    }.

%%====================================================================
%% EUnit Tests
%%====================================================================

-ifdef(TEST).

small_flat_doc_test() ->
    Doc = small_flat_doc(0),
    ?assertEqual(5, map_size(Doc)),
    ?assertEqual(<<"sf_0">>, maps:get(<<"_id">>, Doc)).

medium_flat_doc_test() ->
    Doc = medium_flat_doc(0),
    ?assertEqual(21, map_size(Doc)),
    ?assertEqual(<<"mf_0">>, maps:get(<<"_id">>, Doc)).

large_flat_doc_test() ->
    Doc = large_flat_doc(0),
    ?assert(map_size(Doc) >= 100),
    ?assertEqual(<<"lf_0">>, maps:get(<<"_id">>, Doc)).

small_nested_doc_test() ->
    Doc = small_nested_doc(0),
    ?assertEqual(<<"sn_0">>, maps:get(<<"_id">>, Doc)),
    ?assert(is_map(maps:get(<<"customer">>, Doc))),
    ?assert(is_map(maps:get(<<"shipping">>, Doc))).

medium_nested_doc_test() ->
    Doc = medium_nested_doc(0),
    ?assertEqual(<<"mn_0">>, maps:get(<<"_id">>, Doc)),
    Profile = maps:get(<<"profile">>, Doc),
    ?assert(is_map(maps:get(<<"contact">>, Profile))),
    ?assert(is_map(maps:get(<<"location">>, Profile))).

large_nested_doc_test() ->
    Doc = large_nested_doc(0),
    ?assertEqual(<<"ln_0">>, maps:get(<<"_id">>, Doc)),
    Company = maps:get(<<"company">>, Doc),
    HQ = maps:get(<<"headquarters">>, Company),
    ?assert(is_map(maps:get(<<"address">>, HQ))),
    ?assert(is_map(maps:get(<<"contact">>, HQ))).

doc_types_test() ->
    Types = doc_types(),
    ?assertEqual(6, length(Types)),
    lists:foreach(fun(Type) ->
        Info = doc_type_info(Type),
        ?assert(is_map(Info)),
        ?assert(is_function(maps:get(generator, Info), 1))
    end, Types).

-endif.
