if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

foreach(required_source
    "src/lc_pouch.h"
    "src/lc_pouch.c"
    "src/lc_pouch_namespace.h"
    "src/lc_pouch_namespace.c"
    "src/lc_pouch_path.h"
    "src/lc_pouch_path.c"
    "src/lc_pouch_client.c"
    "deprecated/pouch-legacy/src/lc_pouch.c"
)
    if(NOT EXISTS "${LOCKDC_ROOT}/${required_source}")
        message(FATAL_ERROR "missing pouch redesign source: ${required_source}")
    endif()
endforeach()

foreach(retired_source
    "src/lc_pouch_store.h"
    "src/lc_pouch_allocator.c"
    "src/lc_pouch_logstore.c"
    "src/lc_pouch_index.c"
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
    "src/lc_pouch_index.c"
)
    string(FIND "${root_cmake}" "${forbidden}" forbidden_index)
    if(NOT forbidden_index EQUAL -1)
        message(FATAL_ERROR "retired pouch source is still referenced by CMake: ${forbidden}")
    endif()
endforeach()
