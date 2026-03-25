file(READ "${LOCKDC_ROOT}/docker-compose.yaml" compose_yaml)

if(NOT compose_yaml MATCHES "lockd-disk-a-config:/config")
    message(FATAL_ERROR "docker-compose.yaml no longer mounts lockd-disk-a-config as the shared disk config root")
endif()

if(NOT compose_yaml MATCHES "lockd-disk-a,lockd-disk-b,localhost,127\\.0\\.0\\.1")
    message(FATAL_ERROR "docker-compose.yaml must generate a shared disk server certificate valid for both disk nodes")
endif()

if(compose_yaml MATCHES "lockd-disk-b-config:/config")
    message(FATAL_ERROR "docker-compose.yaml must not mount an independent lockd-disk-b-config into the shared disk runtime")
endif()
