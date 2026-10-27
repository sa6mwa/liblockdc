# Explicitly requested diagnostics only: never built by all, tests, benchmarks,
# packaging or release gates. Page residency/I/O accounting are Linux-specific.
if(TARGET lc_static AND CMAKE_SYSTEM_NAME STREQUAL "Linux" AND
   CMAKE_SYSTEM_PROCESSOR STREQUAL CMAKE_HOST_SYSTEM_PROCESSOR)
  add_executable(lockdc_collection_read_compare EXCLUDE_FROM_ALL
    collection_read_compare.c)
  target_link_libraries(lockdc_collection_read_compare PRIVATE lc_static)
  lc_configure_c_target(lockdc_collection_read_compare)
endif()
