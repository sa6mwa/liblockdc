if(NOT DEFINED LOCKDC_ROOT OR LOCKDC_ROOT STREQUAL "")
  message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

set(lockdc_storage_owned_sources
  "${LOCKDC_ROOT}/src/lc_pouch.h"
  "${LOCKDC_ROOT}/src/lc_pouch.c"
  "${LOCKDC_ROOT}/src/lc_pouch_namespace.c"
  "${LOCKDC_ROOT}/src/lc_pouch_path.c"
)

foreach(lockdc_storage_source IN LISTS lockdc_storage_owned_sources)
  if(NOT EXISTS "${lockdc_storage_source}")
    message(FATAL_ERROR "missing pouch storage source: ${lockdc_storage_source}")
  endif()
  file(READ "${lockdc_storage_source}" lockdc_storage_text)
  foreach(forbidden_pattern
      "#[ \t]*include[ \t]*[<\"]lql/"
      "\\blql_"
      "\\bLQL_")
    if(lockdc_storage_text MATCHES "${forbidden_pattern}")
      message(FATAL_ERROR
        "pouch storage/index files must not depend on liblql semantics: ${lockdc_storage_source} matches ${forbidden_pattern}")
    endif()
  endforeach()
endforeach()
