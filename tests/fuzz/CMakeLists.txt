if(NOT TARGET lc_static)
  message(FATAL_ERROR "Fuzz targets require lc_static.")
endif()

function(lc_add_fuzz_target target_name)
  add_executable(${target_name} ${ARGN})
  target_link_libraries(${target_name} PRIVATE lc_static)
  target_include_directories(${target_name} PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/src)
  set_target_properties(${target_name}
    PROPERTIES
      C_STANDARD 99
      C_STANDARD_REQUIRED ON
      C_EXTENSIONS ON
  )
  target_compile_options(${target_name} PRIVATE
    $<$<COMPILE_LANG_AND_ID:C,Clang>:-fsanitize=fuzzer,address,undefined -fno-omit-frame-pointer -g>
  )
  target_link_options(${target_name} PRIVATE
    $<$<LINK_LANGUAGE:C>:-fsanitize=fuzzer,address,undefined>
  )
  target_compile_definitions(${target_name} PRIVATE FUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION=1)
endfunction()

lc_add_fuzz_target(lc_fuzz_streams fuzz_lc_streams.c)
lc_add_fuzz_target(lc_fuzz_bundle_open fuzz_lc_bundle_open.c)
lc_add_fuzz_target(lc_fuzz_attachment_decode fuzz_lc_attachment_decode.c)
lc_add_fuzz_target(lc_fuzz_queue_meta fuzz_lc_queue_meta.c)
lc_add_fuzz_target(lc_fuzz_mutate_parse fuzz_lc_mutate_parse.c)
lc_add_fuzz_target(lc_fuzz_mutate_apply fuzz_lc_mutate_apply.c)

set(LC_FUZZ_BINARY_CORPUS_DIR "${CMAKE_CURRENT_BINARY_DIR}/corpus")
file(REMOVE_RECURSE "${LC_FUZZ_BINARY_CORPUS_DIR}")
file(MAKE_DIRECTORY "${LC_FUZZ_BINARY_CORPUS_DIR}")
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/corpus/streams" DESTINATION "${LC_FUZZ_BINARY_CORPUS_DIR}")
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/corpus/bundle_open" DESTINATION "${LC_FUZZ_BINARY_CORPUS_DIR}")
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/corpus/attachment_decode" DESTINATION "${LC_FUZZ_BINARY_CORPUS_DIR}")
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/corpus/queue_meta" DESTINATION "${LC_FUZZ_BINARY_CORPUS_DIR}")
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/corpus/mutate_parse" DESTINATION "${LC_FUZZ_BINARY_CORPUS_DIR}")
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/corpus/mutate_apply" DESTINATION "${LC_FUZZ_BINARY_CORPUS_DIR}")

add_test(NAME lc_fuzz_streams_smoke COMMAND lc_fuzz_streams -runs=128 ${LC_FUZZ_BINARY_CORPUS_DIR}/streams)
set_tests_properties(lc_fuzz_streams_smoke PROPERTIES LABELS fuzz)

add_test(NAME lc_fuzz_bundle_open_smoke COMMAND lc_fuzz_bundle_open -runs=128 ${LC_FUZZ_BINARY_CORPUS_DIR}/bundle_open)
set_tests_properties(lc_fuzz_bundle_open_smoke PROPERTIES LABELS fuzz)

add_test(NAME lc_fuzz_attachment_decode_smoke COMMAND lc_fuzz_attachment_decode -runs=128 ${LC_FUZZ_BINARY_CORPUS_DIR}/attachment_decode)
set_tests_properties(lc_fuzz_attachment_decode_smoke PROPERTIES LABELS fuzz)
set_tests_properties(lc_fuzz_attachment_decode_smoke PROPERTIES ENVIRONMENT "ASAN_OPTIONS=detect_leaks=0")

add_test(NAME lc_fuzz_queue_meta_smoke COMMAND lc_fuzz_queue_meta -runs=128 ${LC_FUZZ_BINARY_CORPUS_DIR}/queue_meta)
set_tests_properties(lc_fuzz_queue_meta_smoke PROPERTIES LABELS fuzz)
set_tests_properties(lc_fuzz_queue_meta_smoke PROPERTIES ENVIRONMENT "ASAN_OPTIONS=detect_leaks=0")

add_test(NAME lc_fuzz_mutate_parse_smoke COMMAND lc_fuzz_mutate_parse -runs=128 ${LC_FUZZ_BINARY_CORPUS_DIR}/mutate_parse)
set_tests_properties(lc_fuzz_mutate_parse_smoke PROPERTIES LABELS fuzz)

add_test(NAME lc_fuzz_mutate_apply_smoke COMMAND lc_fuzz_mutate_apply -runs=128 ${LC_FUZZ_BINARY_CORPUS_DIR}/mutate_apply)
set_tests_properties(lc_fuzz_mutate_apply_smoke PROPERTIES LABELS fuzz)
