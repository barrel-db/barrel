%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Dec 2018 11:02
%%%-------------------------------------------------------------------
-author("benoitc").

-define(INT_MIN, 16#80).
-define(INT_MAX, 16#fd).
-define(INT_MAX_WIDTH, 8).
-define(INT_ZERO, (?INT_MIN + ?INT_MAX_WIDTH) ).
-define(INT_SMALL, (?INT_MAX - ?INT_ZERO - ?INT_MAX_WIDTH) ).


-define(ESCAPE, 16#00).
-define(ESCAPED_TERM, 16#01).
-define(ESCAPED_JSON_OBJECT_KEY_TERM, 16#02).
-define(ESCAPED_JSON_ARRAY, 16#03).
-define(ESCAPED_JSON_ARRAY_KEY, 16#04).
-define(ESCAPED_00, 16#FF).
-define(ESCAPED_FF, 16#00).

-define(BYTES_MARKER, 16#12).
-define(BYTES_MARKER_DESC, (?BYTES_MARKER + 1)).
-define(LITERAL_MARKER, (?BYTES_MARKER_DESC + 1)).
-define(LITERAL_MARKER_DESC, (?LITERAL_MARKER + 1)).


-define(ENCODED_NULL, 16#00).
-define(ENCODED_NOT_NULL, 16#01).
-define(FLOAT_NAN, (?ENCODED_NOT_NULL + 1)).
-define(FLOAT_NEG, (?FLOAT_NAN + 1)).
-define(FLOAT_ZERO, (?FLOAT_NEG + 1)).
-define(FLOAT_POS, (?FLOAT_ZERO + 1)).
-define(FLOAT_NAN_DESC, (?FLOAT_POS + 1)).

-define(JSON_INVERTED_INDEX, (?BYTES_MARKER + 38)).
-define(JSON_EMPTY_ARRAY, (?JSON_INVERTED_INDEX + 1)).
-define(JSON_EMPTY_OBJECT, (?JSON_EMPTY_ARRAY + 1)).