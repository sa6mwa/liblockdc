include("${LOCKDC_ROOT}/tests/bootlin_runtime_test_support.cmake")
include("${LOCKDC_ROOT}/cmake/LcGeneratedPath.cmake")
lockdc_import_cmake_cache_value("${LOCKDC_BINARY_DIR}" CMAKE_TOOLCHAIN_FILE)
lockdc_import_cmake_cache_value("${LOCKDC_BINARY_DIR}" CMAKE_GENERATOR)
set(test_root "${LOCKDC_BINARY_DIR}/optional-postgres-test")
lockdc_assert_generated_path("${LOCKDC_ROOT}" "${test_root}")
file(REMOVE_RECURSE "${test_root}")
set(deps "${test_root}/deps")
file(MAKE_DIRECTORY "${deps}")
# Copy the provider SDK as an independent prefix: producer validation rejects a
# symlink prefix whose imported targets resolve outside that prefix. Expose the
# other prepared components from this checkout without exposing any database SDK.
file(COPY "${LOCKDC_EXTERNAL_ROOT}/cpkt" DESTINATION "${deps}")
foreach(component openssl zlib curl nghttp2 libssh2 pslog lonejson liblql)
    file(CREATE_LINK "${LOCKDC_EXTERNAL_ROOT}/${component}" "${deps}/${component}"
        SYMBOLIC RESULT result)
    if(NOT result STREQUAL "0")
        message(FATAL_ERROR "Could not expose ${component} SDK: ${result}")
    endif()
endforeach()
if(EXISTS "${deps}/cpktdb")
    message(FATAL_ERROR "Fixture unexpectedly contains cpktdb")
endif()
set(configure_args
    -S "${LOCKDC_ROOT}" -G "${CMAKE_GENERATOR}"
    "-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}"
    "-DLOCKDC_EXTERNAL_ROOT=${deps}"
    -DLOCKDC_BUILD_TESTS=ON -DLOCKDC_BUILD_EXAMPLES=OFF
    -DLOCKDC_BUILD_BENCHMARKS=OFF -DLOCKDC_BUILD_LUA_BINDINGS=OFF
    -DLOCKDC_BUILD_E2E_TESTS=OFF -DLOCKDC_BUILD_FUZZERS=OFF)
execute_process(COMMAND "${CMAKE_COMMAND}" -E env --unset=LOCKDC_BUILD_POSTGRES
    "${CMAKE_COMMAND}" ${configure_args}
    -B "${test_root}/core"
    -DCMAKE_DISABLE_FIND_PACKAGE_CpktPostgres=TRUE
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(NOT result EQUAL 0)
    message(FATAL_ERROR "Core configure required absent cpktdb\n${output}\n${error}")
endif()
file(STRINGS "${test_root}/core/CMakeCache.txt" postgres_default
    REGEX "^LOCKDC_BUILD_POSTGRES:BOOL=")
if(NOT postgres_default STREQUAL "LOCKDC_BUILD_POSTGRES:BOOL=OFF")
    message(FATAL_ERROR "PostgreSQL must be disabled by default: ${postgres_default}")
endif()
file(READ "${test_root}/core/tests/unit/CTestTestfile.cmake" unit_tests)
if(unit_tests MATCHES "lc_unit_cpkt_postgres_checked")
    message(FATAL_ERROR "Disabled PostgreSQL still registered its facade test")
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" ${configure_args}
    -B "${test_root}/provider" -DLOCKDC_BUILD_POSTGRES=ON
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(result EQUAL 0 OR NOT "${output}${error}" MATCHES "CpktPostgres")
    message(FATAL_ERROR "Missing optional provider SDK was not rejected\n${output}\n${error}")
endif()

# A clean release selects the same capability for SDK provisioning and each
# fresh library configuration. The fixture deliberately has no database SDK.
execute_process(COMMAND "${CMAKE_COMMAND}" -E env LOCKDC_BUILD_POSTGRES=ON
    "${CMAKE_COMMAND}" ${configure_args} -B "${test_root}/env-on"
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(result EQUAL 0 OR NOT "${output}${error}" MATCHES "CpktPostgres")
    message(FATAL_ERROR "Clean configure ignored PostgreSQL selection\n${output}\n${error}")
endif()
foreach(mode env-off explicit-off)
    if(mode STREQUAL "env-off")
        set(environment_selection OFF)
        set(explicit_selection)
    else()
        set(environment_selection ON)
        set(explicit_selection -DLOCKDC_BUILD_POSTGRES=OFF)
    endif()
    execute_process(COMMAND "${CMAKE_COMMAND}" -E env
        "LOCKDC_BUILD_POSTGRES=${environment_selection}"
        "${CMAKE_COMMAND}" ${configure_args} -B "${test_root}/${mode}"
        ${explicit_selection}
        RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
    if(NOT result EQUAL 0)
        message(FATAL_ERROR "Core-only configure failed for ${mode}\n${output}\n${error}")
    endif()
    file(STRINGS "${test_root}/${mode}/CMakeCache.txt" selection
        REGEX "^LOCKDC_BUILD_POSTGRES:BOOL=")
    if(NOT selection STREQUAL "LOCKDC_BUILD_POSTGRES:BOOL=OFF")
        message(FATAL_ERROR "Core-only selection changed for ${mode}: ${selection}")
    endif()
endforeach()
execute_process(COMMAND "${CMAKE_COMMAND}" -E env LOCKDC_BUILD_POSTGRES=invalid
    "${CMAKE_COMMAND}" ${configure_args} -B "${test_root}/env-invalid"
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(result EQUAL 0 OR NOT "${output}${error}" MATCHES "must be ON or OFF")
    message(FATAL_ERROR "Invalid clean-build selection was not rejected\n${output}\n${error}")
endif()

foreach(selection OFF ON)
    execute_process(COMMAND "${CMAKE_COMMAND}" -E env
        LOCKDC_DEPS_DRY_RUN=1 "LOCKDC_BUILD_POSTGRES=${selection}"
        bash "${LOCKDC_ROOT}/scripts/deps.sh" deps-x86_64-linux-gnu
        RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
    if(NOT result EQUAL 0 OR NOT output MATCHES "postgres=${selection}")
        message(FATAL_ERROR "Dependency selection mismatch\n${output}\n${error}")
    endif()
endforeach()
execute_process(COMMAND "${CMAKE_COMMAND}" -E env
    --unset=LOCKDC_BUILD_POSTGRES LOCKDC_DEPS_DRY_RUN=1
    bash "${LOCKDC_ROOT}/scripts/deps.sh" deps-x86_64-linux-gnu
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(NOT result EQUAL 0 OR NOT output MATCHES "postgres=OFF")
    message(FATAL_ERROR "Dependency preparation must default to core-only\n${output}\n${error}")
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" -E env LOCKDC_DEPS_DRY_RUN=1
    LOCKDC_BUILD_POSTGRES=invalid bash "${LOCKDC_ROOT}/scripts/deps.sh"
    deps-x86_64-linux-gnu RESULT_VARIABLE result
    OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(result EQUAL 0 OR NOT error MATCHES "must be ON or OFF")
    message(FATAL_ERROR "Invalid provider selection was not rejected\n${output}\n${error}")
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" -E env LOCKDC_DEPS_DRY_RUN=1
    LOCKDC_BUILD_POSTGRES=OFF bash "${LOCKDC_ROOT}/scripts/deps.sh"
    deps-x86_64-linux-gnu --with-postgres
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(NOT result EQUAL 0 OR NOT output MATCHES "postgres=ON")
    message(FATAL_ERROR "Explicit provider selection failed\n${output}\n${error}")
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" -E env LOCKDC_DEPS_DRY_RUN=1
    LOCKDC_BUILD_POSTGRES=ON bash "${LOCKDC_ROOT}/scripts/deps.sh"
    deps-x86_64-linux-gnu --without-postgres
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(NOT result EQUAL 0 OR NOT output MATCHES "postgres=OFF")
    message(FATAL_ERROR "Explicit provider disablement failed\n${output}\n${error}")
endif()

foreach(binary "${LOCKDC_CORE_STATIC}" "${LOCKDC_CORE_SHARED}")
    execute_process(COMMAND "${LOCKDC_NM}" -u "${binary}"
        RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
    if(NOT result EQUAL 0 OR output MATCHES "cpkt_postgres_|[ \t]_?PQ[A-Za-z0-9_]+")
        message(FATAL_ERROR "Core has native PostgreSQL references\n${output}\n${error}")
    endif()
endforeach()
execute_process(COMMAND "${LOCKDC_READELF}" -d "${LOCKDC_CORE_SHARED}"
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(NOT result EQUAL 0 OR output MATCHES "NEEDED[^\n]*(libpq|libcpkt_postgres)")
    message(FATAL_ERROR "Core has a mandatory PostgreSQL loader dependency\n${output}\n${error}")
endif()
