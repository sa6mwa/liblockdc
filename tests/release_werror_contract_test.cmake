if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

file(READ "${LOCKDC_ROOT}/CMakePresets.json" lockdc_presets)
file(READ "${LOCKDC_ROOT}/CMakeLists.txt" lockdc_cmake)

function(count_occurrences haystack needle out_var)
    string(LENGTH "${haystack}" haystack_length)
    string(LENGTH "${needle}" needle_length)
    string(REPLACE "${needle}" "" shortened_haystack "${haystack}")
    string(LENGTH "${shortened_haystack}" shortened_length)
    math(EXPR removed_length "${haystack_length} - ${shortened_length}")
    math(EXPR count "${removed_length} / ${needle_length}")
    set(${out_var} "${count}" PARENT_SCOPE)
endfunction()

string(FIND "${lockdc_presets}" "\"name\": \"x86_64-linux-gnu-release\"" release_preset_index)
if(release_preset_index EQUAL -1)
    message(FATAL_ERROR "Expected x86_64-linux-gnu-release preset to exist")
endif()

string(FIND "${lockdc_presets}" "\"LOCKDC_RELEASE_WERROR\": \"ON\"" release_werror_index)
if(release_werror_index EQUAL -1)
    message(FATAL_ERROR
        "Expected release presets to pin LOCKDC_RELEASE_WERROR=ON")
endif()

if(release_werror_index LESS release_preset_index)
    message(FATAL_ERROR
        "Expected LOCKDC_RELEASE_WERROR=ON to be configured by the release preset family")
endif()

string(FIND "${lockdc_cmake}"
    "option(LOCKDC_RELEASE_WERROR \"Treat warnings as errors for lockdc release builds.\" ON)"
    release_option_index)
if(release_option_index EQUAL -1)
    message(FATAL_ERROR "Expected LOCKDC_RELEASE_WERROR to default to ON")
endif()

string(FIND "${lockdc_cmake}"
    "if(LOCKDC_RELEASE_WERROR AND CMAKE_BUILD_TYPE STREQUAL \"Release\")"
    release_gate_index)
if(release_gate_index EQUAL -1)
    message(FATAL_ERROR "Expected release C target helpers to gate -Werror on Release builds")
endif()
count_occurrences("${lockdc_cmake}"
    "if(LOCKDC_RELEASE_WERROR AND CMAKE_BUILD_TYPE STREQUAL \"Release\")"
    release_gate_count)
if(release_gate_count LESS 2)
    message(FATAL_ERROR
        "Expected both release C target helpers to gate -Werror on Release builds")
endif()

string(FIND "${lockdc_cmake}"
    [=[target_compile_options(${target} PRIVATE -Werror)]=]
    werror_option_index)
if(werror_option_index EQUAL -1)
    message(FATAL_ERROR "Expected release C target helpers to add -Werror")
endif()
count_occurrences("${lockdc_cmake}"
    [=[target_compile_options(${target} PRIVATE -Werror)]=]
    werror_option_count)
if(werror_option_count LESS 2)
    message(FATAL_ERROR
        "Expected both release C target helpers to add -Werror")
endif()
