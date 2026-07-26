if(NOT TARGET lc_static)
  message(FATAL_ERROR "Fuzz targets require lc_static.")
endif()

function(lc_add_fuzz_target target_name)
  add_executable(${target_name} afl_fuzzer_main.c ${ARGN})
  target_link_libraries(${target_name} PRIVATE lc_static)
  target_include_directories(${target_name} PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/src)
  lc_configure_c_test_target(${target_name})
  target_compile_definitions(${target_name} PRIVATE FUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION=1)
endfunction()

lc_add_fuzz_target(lc_fuzz_streams fuzz_lc_streams.c)
lc_add_fuzz_target(lc_fuzz_bundle_open fuzz_lc_bundle_open.c)
lc_add_fuzz_target(lc_fuzz_attachment_decode fuzz_lc_attachment_decode.c)
lc_add_fuzz_target(lc_fuzz_queue_meta fuzz_lc_queue_meta.c)
lc_add_fuzz_target(lc_fuzz_query_keys_stream fuzz_lc_query_keys_stream.c)
lc_add_fuzz_target(lc_fuzz_mutate_parse fuzz_lc_mutate_parse.c)
lc_add_fuzz_target(lc_fuzz_mutate_apply fuzz_lc_mutate_apply.c)
lc_add_fuzz_target(lc_fuzz_pouch_lql_plan fuzz_lc_pouch_lql_plan.c)

set(LC_FUZZ_BINARY_CORPUS_DIR "${CMAKE_CURRENT_BINARY_DIR}/corpus")
file(REMOVE_RECURSE "${LC_FUZZ_BINARY_CORPUS_DIR}")
file(MAKE_DIRECTORY "${LC_FUZZ_BINARY_CORPUS_DIR}")
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/corpus/streams" DESTINATION "${LC_FUZZ_BINARY_CORPUS_DIR}")
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/corpus/bundle_open" DESTINATION "${LC_FUZZ_BINARY_CORPUS_DIR}")
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/corpus/attachment_decode" DESTINATION "${LC_FUZZ_BINARY_CORPUS_DIR}")
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/corpus/queue_meta" DESTINATION "${LC_FUZZ_BINARY_CORPUS_DIR}")
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/corpus/query_keys_stream" DESTINATION "${LC_FUZZ_BINARY_CORPUS_DIR}")
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/corpus/mutate_parse" DESTINATION "${LC_FUZZ_BINARY_CORPUS_DIR}")
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/corpus/mutate_apply" DESTINATION "${LC_FUZZ_BINARY_CORPUS_DIR}")
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/corpus/pouch_lql_plan" DESTINATION "${LC_FUZZ_BINARY_CORPUS_DIR}")

add_test(NAME lc_fuzz_streams_smoke COMMAND lc_fuzz_streams ${LC_FUZZ_BINARY_CORPUS_DIR}/streams/seed.json)
set_tests_properties(lc_fuzz_streams_smoke PROPERTIES LABELS fuzz)

add_test(NAME lc_fuzz_bundle_open_smoke COMMAND lc_fuzz_bundle_open ${LC_FUZZ_BINARY_CORPUS_DIR}/bundle_open/seed.pem)
set_tests_properties(lc_fuzz_bundle_open_smoke PROPERTIES LABELS fuzz)

add_test(NAME lc_fuzz_attachment_decode_smoke COMMAND lc_fuzz_attachment_decode ${LC_FUZZ_BINARY_CORPUS_DIR}/attachment_decode/attach.json)
set_tests_properties(lc_fuzz_attachment_decode_smoke PROPERTIES LABELS fuzz)

add_test(NAME lc_fuzz_queue_meta_smoke COMMAND lc_fuzz_queue_meta ${LC_FUZZ_BINARY_CORPUS_DIR}/queue_meta/meta.json)
set_tests_properties(lc_fuzz_queue_meta_smoke PROPERTIES LABELS fuzz)

add_test(NAME lc_fuzz_query_keys_stream_smoke COMMAND lc_fuzz_query_keys_stream ${LC_FUZZ_BINARY_CORPUS_DIR}/query_keys_stream/two_keys)
set_tests_properties(lc_fuzz_query_keys_stream_smoke PROPERTIES LABELS fuzz)

add_test(NAME lc_fuzz_mutate_parse_smoke COMMAND lc_fuzz_mutate_parse ${LC_FUZZ_BINARY_CORPUS_DIR}/mutate_parse/02d6b5dc5e0c4b854ad463736de6ba15229e10fa)
set_tests_properties(lc_fuzz_mutate_parse_smoke PROPERTIES LABELS fuzz)

add_test(NAME lc_fuzz_mutate_apply_smoke COMMAND lc_fuzz_mutate_apply ${LC_FUZZ_BINARY_CORPUS_DIR}/mutate_apply/105064699aa30295ea0a31a6c95d2a7775a230cb)
set_tests_properties(lc_fuzz_mutate_apply_smoke PROPERTIES LABELS fuzz)

add_test(NAME lc_fuzz_pouch_lql_plan_smoke COMMAND lc_fuzz_pouch_lql_plan ${LC_FUZZ_BINARY_CORPUS_DIR}/pouch_lql_plan/compound.json)
set_tests_properties(lc_fuzz_pouch_lql_plan_smoke PROPERTIES LABELS fuzz)
