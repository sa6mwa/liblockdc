if(NOT DEFINED LOCKDC_LIBSSH2_SOURCE_DIR OR LOCKDC_LIBSSH2_SOURCE_DIR STREQUAL "")
  message(FATAL_ERROR "LOCKDC_LIBSSH2_SOURCE_DIR is required")
endif()

set(libssh2_cmake "${LOCKDC_LIBSSH2_SOURCE_DIR}/src/CMakeLists.txt")
if(NOT EXISTS "${libssh2_cmake}")
  message(FATAL_ERROR "missing libssh2 source file: ${libssh2_cmake}")
endif()

file(READ "${libssh2_cmake}" libssh2_cmake_text)

set(old_block [=[unset(_libssh2_export)

# we want it to be called libssh2 on all platforms
if(BUILD_STATIC_LIBS)
  list(APPEND _libssh2_export ${LIB_STATIC})
  add_library(${LIB_STATIC} STATIC ${_sources})
  add_library(${PROJECT_NAME}::${LIB_STATIC} ALIAS ${LIB_STATIC})
  target_compile_definitions(${LIB_STATIC} PRIVATE ${PRIVATE_COMPILE_DEFINITIONS} ${_libssh2_definitions})
  target_link_libraries(${LIB_STATIC} PRIVATE ${LIBSSH2_LIBS})
  set_target_properties(${LIB_STATIC} PROPERTIES
    PREFIX "" OUTPUT_NAME "libssh2" SOVERSION "${_libssh2_soversion}" VERSION "${_libssh2_libversion}"
    SUFFIX "${STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}")

  target_include_directories(${LIB_STATIC}
    PRIVATE
      "${PROJECT_SOURCE_DIR}/include"
      ${libssh2_INCLUDE_DIRS}
      ${PRIVATE_INCLUDE_DIRECTORIES}
    PUBLIC
      "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>"
      "$<INSTALL_INTERFACE:$<INSTALL_PREFIX>/${CMAKE_INSTALL_INCLUDEDIR}>")
endif()
if(BUILD_SHARED_LIBS)
  list(APPEND _libssh2_export ${LIB_SHARED})
  add_library(${LIB_SHARED} SHARED ${_sources})
  add_library(${PROJECT_NAME}::${LIB_SHARED} ALIAS ${LIB_SHARED})
  if(WIN32)
    set_property(TARGET ${LIB_SHARED} APPEND PROPERTY SOURCES "libssh2.rc")
  endif()
  target_compile_definitions(${LIB_SHARED} PRIVATE ${PRIVATE_COMPILE_DEFINITIONS} ${_libssh2_definitions} ${LIB_SHARED_DEFINITIONS})
  target_compile_options(${LIB_SHARED} PRIVATE ${LIB_SHARED_C_FLAGS})
  target_link_libraries(${LIB_SHARED} PRIVATE ${LIBSSH2_LIBS})
  set_target_properties(${LIB_SHARED} PROPERTIES
    PREFIX "" OUTPUT_NAME "libssh2" SOVERSION "${_libssh2_soversion}" VERSION "${_libssh2_libversion}"
    IMPORT_PREFIX "" IMPORT_SUFFIX "${IMPORT_LIB_SUFFIX}${CMAKE_IMPORT_LIBRARY_SUFFIX}"
    POSITION_INDEPENDENT_CODE ON)

  target_include_directories(${LIB_SHARED}
    PRIVATE
      "${PROJECT_SOURCE_DIR}/include"
      ${libssh2_INCLUDE_DIRS}
      ${PRIVATE_INCLUDE_DIRECTORIES}
    PUBLIC
      "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>"
      "$<INSTALL_INTERFACE:$<INSTALL_PREFIX>/${CMAKE_INSTALL_INCLUDEDIR}>")
endif()
]=])

set(new_block [=[unset(_libssh2_export)

set(_libssh2_library_private_includes
  "${PROJECT_SOURCE_DIR}/include"
  ${libssh2_INCLUDE_DIRS}
  ${PRIVATE_INCLUDE_DIRECTORIES})
set(_libssh2_library_public_includes
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>"
  "$<INSTALL_INTERFACE:$<INSTALL_PREFIX>/${CMAKE_INSTALL_INCLUDEDIR}>")
set(_libssh2_sources_for_targets ${_sources})

if(NOT WIN32 AND BUILD_STATIC_LIBS AND BUILD_SHARED_LIBS)
  add_library(libssh2_object OBJECT ${_sources})
  target_compile_definitions(libssh2_object PRIVATE
    ${PRIVATE_COMPILE_DEFINITIONS}
    ${_libssh2_definitions}
    ${LIB_SHARED_DEFINITIONS})
  target_compile_options(libssh2_object PRIVATE ${LIB_SHARED_C_FLAGS})
  set_target_properties(libssh2_object PROPERTIES POSITION_INDEPENDENT_CODE ON)
  target_include_directories(libssh2_object
    PRIVATE ${_libssh2_library_private_includes}
    PUBLIC ${_libssh2_library_public_includes})
  set(_libssh2_sources_for_targets $<TARGET_OBJECTS:libssh2_object>)
endif()

# we want it to be called libssh2 on all platforms
if(BUILD_STATIC_LIBS)
  list(APPEND _libssh2_export ${LIB_STATIC})
  add_library(${LIB_STATIC} STATIC ${_libssh2_sources_for_targets})
  add_library(${PROJECT_NAME}::${LIB_STATIC} ALIAS ${LIB_STATIC})
  if(NOT TARGET libssh2_object)
    target_compile_definitions(${LIB_STATIC} PRIVATE ${PRIVATE_COMPILE_DEFINITIONS} ${_libssh2_definitions})
  endif()
  target_link_libraries(${LIB_STATIC} PRIVATE ${LIBSSH2_LIBS})
  set_target_properties(${LIB_STATIC} PROPERTIES
    PREFIX "" OUTPUT_NAME "libssh2" SOVERSION "${_libssh2_soversion}" VERSION "${_libssh2_libversion}"
    SUFFIX "${STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}")

  target_include_directories(${LIB_STATIC}
    PRIVATE ${_libssh2_library_private_includes}
    PUBLIC ${_libssh2_library_public_includes})
endif()
if(BUILD_SHARED_LIBS)
  list(APPEND _libssh2_export ${LIB_SHARED})
  add_library(${LIB_SHARED} SHARED ${_libssh2_sources_for_targets})
  add_library(${PROJECT_NAME}::${LIB_SHARED} ALIAS ${LIB_SHARED})
  if(WIN32)
    set_property(TARGET ${LIB_SHARED} APPEND PROPERTY SOURCES "libssh2.rc")
  endif()
  if(NOT TARGET libssh2_object)
    target_compile_definitions(${LIB_SHARED} PRIVATE ${PRIVATE_COMPILE_DEFINITIONS} ${_libssh2_definitions} ${LIB_SHARED_DEFINITIONS})
    target_compile_options(${LIB_SHARED} PRIVATE ${LIB_SHARED_C_FLAGS})
    set_target_properties(${LIB_SHARED} PROPERTIES POSITION_INDEPENDENT_CODE ON)
  endif()
  target_link_libraries(${LIB_SHARED} PRIVATE ${LIBSSH2_LIBS})
  set_target_properties(${LIB_SHARED} PROPERTIES
    PREFIX "" OUTPUT_NAME "libssh2" SOVERSION "${_libssh2_soversion}" VERSION "${_libssh2_libversion}"
    IMPORT_PREFIX "" IMPORT_SUFFIX "${IMPORT_LIB_SUFFIX}${CMAKE_IMPORT_LIBRARY_SUFFIX}")

  target_include_directories(${LIB_SHARED}
    PRIVATE ${_libssh2_library_private_includes}
    PUBLIC ${_libssh2_library_public_includes})
endif()
]=])

string(FIND "${libssh2_cmake_text}" "${new_block}" already_patched_at)
if(NOT already_patched_at EQUAL -1)
  return()
endif()

string(FIND "${libssh2_cmake_text}" "${old_block}" old_block_at)
if(old_block_at EQUAL -1)
  message(FATAL_ERROR "failed to find libssh2 single-pass patch anchor in ${libssh2_cmake}")
endif()

string(REPLACE "${old_block}" "${new_block}" libssh2_cmake_text "${libssh2_cmake_text}")
file(WRITE "${libssh2_cmake}" "${libssh2_cmake_text}")
