-module(barrel_ngram_fs_tests).

-include_lib("eunit/include/eunit.hrl").

tmp_dir() ->
    Dir = filename:join(["/tmp", "barrel_ngram_fs_" ++ integer_to_list(erlang:unique_integer([positive]))]),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Dir.

write_file_and_hash_test() ->
    Dir = tmp_dir(),
    Path = filename:join(Dir, "f"),
    Data = crypto:strong_rand_bytes(3 * 1024 * 1024 + 7),
    ?assertEqual(ok, barrel_ngram_fs:write_file(Path, Data)),
    ?assertEqual({ok, Data}, file:read_file(Path)),
    ?assertNot(filelib:is_file(Path ++ ".tmp")),
    Want = binary:encode_hex(crypto:hash(sha256, Data), lowercase),
    ?assertEqual({ok, Want, byte_size(Data)}, barrel_ngram_fs:sha256_file(Path)),
    _ = file:del_dir_r(Dir).

fsync_dir_test() ->
    Dir = tmp_dir(),
    ?assertEqual(ok, barrel_ngram_fs:fsync_dir(Dir)),
    ?assertEqual({error, enoent}, barrel_ngram_fs:fsync_dir(filename:join(Dir, "missing"))),
    _ = file:del_dir_r(Dir).

segment_checksum_test() ->
    Dir = tmp_dir(),
    Path = filename:join(Dir, "s.ngseg"),
    {ok, #{sha256 := Sha, bytes := Bytes}} =
        barrel_ngram_segment:write(Path, #{doc_count => 1, watermark => <<0:96>>,
                                           postings => [{16#616263, [0]}],
                                           entries => [#{key => <<"k">>, hlc => <<1:96>>,
                                                         deleted => false}]}),
    ?assertEqual({ok, Sha, Bytes}, barrel_ngram_fs:sha256_file(Path)),
    {ok, H} = barrel_ngram_segment:open(Path, #{sha256 => Sha}),
    ok = barrel_ngram_segment:close(H),
    ?assertEqual({error, {corrupt_segment, checksum_mismatch}},
                 barrel_ngram_segment:open(Path, #{sha256 => binary:copy(<<"0">>, 64)})),
    _ = file:del_dir_r(Dir).
