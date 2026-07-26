if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

set(pouch_storage_files
    "src/lc_pouch.h"
    "src/lc_pouch.c"
    "src/lc_pouch_namespace.c"
    "src/lc_pouch_path.c"
)

foreach(pouch_storage_file IN LISTS pouch_storage_files)
    set(path "${LOCKDC_ROOT}/${pouch_storage_file}")
    if(NOT EXISTS "${path}")
        message(FATAL_ERROR "Expected pouch storage file to exist: ${pouch_storage_file}")
    endif()

    file(READ "${path}" contents)
    string(REGEX MATCHALL
        "(^|\n)[^\n]*(^|[^A-Za-z0-9_])(malloc|calloc|realloc|free)[ \t]*\\("
        raw_allocator_matches
        "${contents}"
    )

    if(raw_allocator_matches)
        string(REPLACE "\n" "\n  " formatted_matches "${raw_allocator_matches}")
        message(FATAL_ERROR
            "Pouch storage code must use project allocator helpers, not raw "
            "malloc/calloc/realloc/free calls. Matches in ${pouch_storage_file}:\n"
            "  ${formatted_matches}"
        )
    endif()
endforeach()
