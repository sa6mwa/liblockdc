if(NOT DEFINED LOCKDC_ROOT OR LOCKDC_ROOT STREQUAL "")
  message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

set(lockdc_liblql_owner "${LOCKDC_ROOT}/src/lc_pouch_client.c")
if(NOT EXISTS "${lockdc_liblql_owner}")
  message(FATAL_ERROR "missing pouch client source: ${lockdc_liblql_owner}")
endif()

file(READ "${lockdc_liblql_owner}" lockdc_liblql_owner_text)
foreach(required_snippet
    "#include <lql/lql.h>"
    "lql_new"
    "selector_parse_json")
  string(FIND "${lockdc_liblql_owner_text}" "${required_snippet}" required_index)
  if(required_index EQUAL -1)
    message(FATAL_ERROR
      "pouch query semantics must stay routed through liblql in lc_pouch_client.c; missing '${required_snippet}'")
  endif()
endforeach()

set(lockdc_storage_owned_sources
  "${LOCKDC_ROOT}/src/lc_pouch_store.h"
  "${LOCKDC_ROOT}/src/lc_pouch_disk.c"
  "${LOCKDC_ROOT}/src/lc_pouch_allocator.c"
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
