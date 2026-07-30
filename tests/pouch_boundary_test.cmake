if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

foreach(required_source
    "src/lc_pouch.h"
    "src/lc_pouch.c"
    "src/lc_pouch_client.c"
    "src/lc_pouch_format.h"
    "src/lc_pouch_internal.h"
    "src/lc_pouch_namespace.h"
    "src/lc_pouch_namespace.c"
    "src/lc_pouch_path.h"
    "src/lc_pouch_path.c"
    "src/lc_pouch_index.h"
    "src/lc_pouch_index.c"
    "src/lc_pouch_index_doc.c"
    "src/lc_pouch_index_posting.c"
    "src/lc_pouch_index_result.c"
    "src/lc_pouch_index_terms.c"
    "src/lc_pouch_query_index.h"
    "src/lc_pouch_query_index.c"
    "src/lc_pouch_state.c"
)
    if(NOT EXISTS "${LOCKDC_ROOT}/${required_source}")
        message(FATAL_ERROR "missing pouch source: ${required_source}")
    endif()
endforeach()

foreach(retired_source
    "deprecated/pouch-legacy"
    "src/lc_pouch_store.h"
    "src/lc_pouch_allocator.c"
    "src/lc_pouch_logstore.c"
    "src/lc_pouch_logstore.h"
    "src/lc_pouch_index_temporal.c"
    "src/lc_pouch_index_text.c"
    "src/lc_pouch_temporal.c"
    "src/lc_pouch_temporal.h"
)
    if(EXISTS "${LOCKDC_ROOT}/${retired_source}")
        message(FATAL_ERROR "retired pouch source must not remain live: ${retired_source}")
    endif()
endforeach()

file(READ "${LOCKDC_ROOT}/CMakeLists.txt" root_cmake)
foreach(forbidden
    "src/lc_pouch_store.h"
    "src/lc_pouch_allocator.c"
    "src/lc_pouch_logstore.c"
    "src/lc_pouch_logstore.h"
    "src/lc_pouch_index_temporal.c"
    "src/lc_pouch_index_text.c"
    "src/lc_pouch_temporal.c"
    "src/lc_pouch_temporal.h"
)
    string(FIND "${root_cmake}" "${forbidden}" forbidden_index)
    if(NOT forbidden_index EQUAL -1)
        message(FATAL_ERROR "retired pouch source is still referenced by CMake: ${forbidden}")
    endif()
endforeach()

foreach(active_source
    "src/lc_pouch_client.c"
    "src/lc_pouch_state.c"
    "src/lc_pouch_query_index.c"
    "src/lc_pouch_index.c"
    "src/lc_pouch_index_doc.c"
    "src/lc_pouch_index_posting.c"
    "src/lc_pouch_index_result.c"
    "src/lc_pouch_index_terms.c"
)
    string(FIND "${root_cmake}" "${active_source}" active_index)
    if(active_index EQUAL -1)
        message(FATAL_ERROR "active pouch source is missing from CMake: ${active_source}")
    endif()
endforeach()
