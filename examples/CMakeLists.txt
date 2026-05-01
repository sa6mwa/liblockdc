if(TARGET lc_static)
  set(lc_example_link_target lc_static)
  set(lc_example_curl_target lc::curl_static)
  set(lc_example_pslog_target lc::pslog_static)
  set(lc_example_lonejson_target lc::lonejson_static)
elseif(TARGET lc_shared)
  set(lc_example_link_target lc_shared)
  set(lc_example_curl_target lc::curl_shared)
  set(lc_example_pslog_target lc::pslog_shared)
  set(lc_example_lonejson_target lc::lonejson_shared)
else()
  return()
endif()

function(lc_configure_example_target target)
  target_link_libraries(${target} PRIVATE ${lc_example_link_target} ${lc_example_pslog_target})
  lc_configure_c_target(${target})
  target_compile_definitions(${target} PRIVATE
    LC_EXAMPLE_DEFAULT_CLIENT_PEM="${PROJECT_SOURCE_DIR}/devenv/volumes/lockd-disk-a-config/client.pem"
    LC_EXAMPLE_DEFAULT_FETCH_CA="${PROJECT_SOURCE_DIR}/devenv/volumes/lockd-disk-a-config/ca.pem"
    LC_EXAMPLE_DEFAULT_INPUT_JSON="${PROJECT_SOURCE_DIR}/examples/data/state_stream_input.json"
    LC_EXAMPLE_DEFAULT_FETCH_URL="file://${PROJECT_SOURCE_DIR}/examples/data/state_stream_input.json"
  )
endfunction()

function(lc_configure_minimal_example_target target)
  target_link_libraries(${target} PRIVATE ${lc_example_link_target})
  lc_configure_c_target(${target})
  target_compile_definitions(${target} PRIVATE
    LC_EXAMPLE_DEFAULT_CLIENT_PEM="${PROJECT_SOURCE_DIR}/devenv/volumes/lockd-disk-a-config/client.pem"
  )
endfunction()

add_executable(lc_example_acquire_lease_lifecycle acquire_lease_lifecycle.c)
lc_configure_example_target(lc_example_acquire_lease_lifecycle)

add_executable(lc_example_acquire_for_update acquire_for_update.c)
lc_configure_example_target(lc_example_acquire_for_update)

add_executable(lc_example_client_and_lease_methods client_and_lease_methods.c)
lc_configure_example_target(lc_example_client_and_lease_methods)

add_executable(lc_example_state_stream_roundtrip state_stream_roundtrip.c)
lc_configure_example_target(lc_example_state_stream_roundtrip)

add_executable(lc_example_queue_subscribe_stream queue_subscribe_stream.c)
lc_configure_example_target(lc_example_queue_subscribe_stream)

add_executable(lc_example_consumer_service consumer_service.c)
lc_configure_example_target(lc_example_consumer_service)

add_executable(lc_example_startconsumer startconsumer.c)
lc_configure_example_target(lc_example_startconsumer)
target_link_libraries(lc_example_startconsumer PRIVATE Threads::Threads ${lc_example_lonejson_target})

add_executable(lc_example_management_admin management_admin.c)
lc_configure_example_target(lc_example_management_admin)

add_executable(lc_example_curl_fetch_into_lockd curl_fetch_into_lockd.c)
lc_configure_example_target(lc_example_curl_fetch_into_lockd)
target_link_libraries(lc_example_curl_fetch_into_lockd PRIVATE ${lc_example_curl_target})

add_executable(lc_example_local_mutate local_mutate.c)
lc_configure_minimal_example_target(lc_example_local_mutate)
