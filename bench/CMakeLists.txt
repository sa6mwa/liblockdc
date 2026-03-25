if(NOT TARGET lc_static)
    message(FATAL_ERROR "Benchmarks require lc_static.")
endif()

add_executable(lockdc_bench bench_main.c)
target_include_directories(lockdc_bench PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
)
target_link_libraries(lockdc_bench PRIVATE lc_static)
lc_configure_c_target(lockdc_bench)
lc_enable_coverage_if_requested(lockdc_bench)
