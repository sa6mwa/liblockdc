cmake_minimum_required(VERSION 3.20)

if(NOT DEFINED LOCKDC_ROOT)
  message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

file(READ "${LOCKDC_ROOT}/cmake/LcDependencies.cmake" lc_dependencies_text)

set(expected_snippet "DOWNLOAD_NAME \"\${variant}-\${asset_name}\"")
string(FIND "${lc_dependencies_text}" "${expected_snippet}" expected_pos)
if(expected_pos EQUAL -1)
  message(FATAL_ERROR
    "pslog dependency downloads are not variant-isolated.\n"
    "Expected to find: ${expected_snippet}\n"
    "This regression allows static/shared pslog ExternalProject downloads to clobber each other.")
endif()
