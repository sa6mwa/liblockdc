if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

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

string(FIND "${lockdc_cmake}" [=[-Werror]=] werror_option_index)
if(werror_option_index EQUAL -1)
    message(FATAL_ERROR "Expected C target helpers to make warnings fatal")
endif()
count_occurrences("${lockdc_cmake}" [=[-Werror]=] werror_option_count)
if(werror_option_count LESS 2)
    message(FATAL_ERROR
        "Expected both C target helpers to add -Werror")
endif()

string(FIND "${lockdc_cmake}" [=[LINKER:--fatal-warnings]=] elf_fatal_warnings_index)
if(elf_fatal_warnings_index EQUAL -1)
    message(FATAL_ERROR "Expected the C target policy to make ELF linker warnings fatal")
endif()

string(FIND "${lockdc_cmake}" [=[LINKER:-fatal_warnings]=] darwin_fatal_warnings_index)
if(darwin_fatal_warnings_index EQUAL -1)
    message(FATAL_ERROR "Expected the C target policy to make Darwin linker warnings fatal")
endif()

string(FIND "${lockdc_cmake}"
    [=[-pedantic-errors]=]
    pedantic_errors_index)
if(pedantic_errors_index EQUAL -1)
    message(FATAL_ERROR "Expected C89 target helper to make pedantic diagnostics fatal")
endif()

string(FIND "${lockdc_cmake}" "C_STANDARD 90" c_standard_90_index)
if(NOT c_standard_90_index EQUAL -1)
    message(FATAL_ERROR "C89 target policy must use explicit compiler flags, not C_STANDARD 90")
endif()
