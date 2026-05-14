if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

file(READ "${LOCKDC_ROOT}/cmake/lockdcConfig.cmake.in" lockdc_config_template_text)
if(lockdc_config_template_text MATCHES "liblockdc\\.so")
    message(FATAL_ERROR
        "installed CMake package template hardcodes liblockdc.so\n"
        "template:\n${lockdc_config_template_text}")
endif()
if(NOT lockdc_config_template_text MATCHES "@LOCKDC_SHARED_LINK_NAME@")
    message(FATAL_ERROR
        "installed CMake package template does not use the platform-aware shared linker name placeholder\n"
        "template:\n${lockdc_config_template_text}")
endif()
if(NOT lockdc_config_template_text MATCHES "NOT \"@LOCKDC_SHARED_LINK_NAME@\" STREQUAL \"\"")
    message(FATAL_ERROR
        "installed CMake package template does not guard against static-only SDK exports creating a bogus shared target\n"
        "template:\n${lockdc_config_template_text}")
endif()

file(READ "${LOCKDC_ROOT}/CMakeLists.txt" lockdc_root_cmake_text)
foreach(forbidden_snippet
    "SOVERSION \${PROJECT_VERSION_MAJOR}"
    "CMAKE_SHARED_LIBRARY_SUFFIX}.${PROJECT_VERSION}"
    "CMAKE_SHARED_LIBRARY_SUFFIX}.${PROJECT_VERSION_MAJOR}"
)
    string(FIND "${lockdc_root_cmake_text}" "${forbidden_snippet}" forbidden_index)
    if(NOT forbidden_index EQUAL -1)
        message(FATAL_ERROR
            "CMakeLists.txt still synthesizes an ELF-only shared-library filename fragment '${forbidden_snippet}'\n")
    endif()
endforeach()
if(NOT lockdc_root_cmake_text MATCHES "SOVERSION [$][{]LOCKDC_ABI_VERSION[}]")
    message(FATAL_ERROR
        "CMakeLists.txt must use LOCKDC_ABI_VERSION for the shared-library SOVERSION\n")
endif()

foreach(forbidden_helper
    "${LOCKDC_ROOT}/cmake/install_vendored_sdk_dev_artifacts.cmake.in"
    "${LOCKDC_ROOT}/cmake/install_vendored_sdk_runtime_artifacts.cmake.in")
    if(EXISTS "${forbidden_helper}")
        message(FATAL_ERROR "obsolete vendored install helper still exists: ${forbidden_helper}")
    endif()
endforeach()
