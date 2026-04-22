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

file(READ "${LOCKDC_ROOT}/CMakeLists.txt" lockdc_root_cmake_text)
foreach(forbidden_snippet
    "CMAKE_SHARED_LIBRARY_SUFFIX}.${PROJECT_VERSION}"
    "CMAKE_SHARED_LIBRARY_SUFFIX}.${PROJECT_VERSION_MAJOR}"
)
    string(FIND "${lockdc_root_cmake_text}" "${forbidden_snippet}" forbidden_index)
    if(NOT forbidden_index EQUAL -1)
        message(FATAL_ERROR
            "CMakeLists.txt still synthesizes an ELF-only shared-library filename fragment '${forbidden_snippet}'\n")
    endif()
endforeach()

file(READ "${LOCKDC_ROOT}/cmake/install_vendored_sdk_runtime_artifacts.cmake.in"
     lockdc_runtime_helper_template_text)
foreach(required_snippet
    "@CMAKE_SHARED_LIBRARY_SUFFIX@*"
    "*@CMAKE_SHARED_LIBRARY_SUFFIX@"
)
    string(FIND "${lockdc_runtime_helper_template_text}" "${required_snippet}" snippet_index)
    if(snippet_index EQUAL -1)
        message(FATAL_ERROR
            "runtime vendored install helper template is missing portable shared-library glob fragment '${required_snippet}'\n"
            "template:\n${lockdc_runtime_helper_template_text}")
    endif()
endforeach()
