set(pouch_sources
    "${LOCKDC_ROOT}/src/lc_pouch_client.c"
    "${LOCKDC_ROOT}/src/lc_pouch_disk.c"
)

set(raw_alloc_pattern "(^|[^A-Za-z0-9_])(malloc|calloc|realloc|free)[ \t\r\n]*\\(")

foreach(path IN LISTS pouch_sources)
    if(NOT EXISTS "${path}")
        message(FATAL_ERROR "missing pouch source file: ${path}")
    endif()
    file(READ "${path}" source_text)
    string(REGEX MATCH "${raw_alloc_pattern}" raw_alloc_match "${source_text}")
    if(raw_alloc_match)
        message(FATAL_ERROR
            "pouch implementation must not call malloc/calloc/realloc/free directly: ${path}\n"
            "Use lc_pouch_* allocation for storage-owned data or lc_*_local helpers for public result ownership.")
    endif()
endforeach()
