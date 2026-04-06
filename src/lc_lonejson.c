#include "lc_api_internal.h"
#include "lc_internal.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>

static const char *lc_lonejson_detail_message(const lonejson_error *error,
                                              const char *fallback) {
  if (error != NULL && error->message[0] != '\0') {
    return error->message;
  }
  return fallback;
}

int lc_lonejson_error_from_status(lc_error *error, lonejson_status status,
                                  const lonejson_error *lj_error,
                                  const char *message) {
  int code;

  switch (status) {
  case LONEJSON_STATUS_OK:
    return LC_OK;
  case LONEJSON_STATUS_INVALID_ARGUMENT:
    code = LC_ERR_INVALID;
    break;
  case LONEJSON_STATUS_ALLOCATION_FAILED:
    code = LC_ERR_NOMEM;
    break;
  default:
    code = LC_ERR_PROTOCOL;
    break;
  }
  return lc_error_set(error, code, 0L, message,
                      lc_lonejson_detail_message(lj_error, NULL), NULL, NULL);
}

int lc_engine_lonejson_error_from_status(lc_engine_error *error,
                                         lonejson_status status,
                                         const lonejson_error *lj_error,
                                         const char *message) {
  switch (status) {
  case LONEJSON_STATUS_OK:
    return LC_ENGINE_OK;
  case LONEJSON_STATUS_INVALID_ARGUMENT:
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        message != NULL ? message
                        : lc_lonejson_detail_message(
                              lj_error, "invalid lonejson argument"));
  case LONEJSON_STATUS_ALLOCATION_FAILED:
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        message != NULL ? message
                        : lc_lonejson_detail_message(
                              lj_error, "lonejson allocation failed"));
  default:
    return lc_engine_set_protocol_error(
        error, message != NULL ? message
                               : lc_lonejson_detail_message(
                                     lj_error, "failed to parse JSON"));
  }
}

int lc_engine_file_write_callback(void *context, const void *bytes,
                                  size_t count, lc_engine_error *error) {
  FILE *fp;

  fp = (FILE *)context;
  if (fp == NULL || (count > 0U && bytes == NULL)) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "file write callback requires file and bytes");
  }
  if (count > 0U && fwrite(bytes, 1U, count, fp) != count) {
    if (ferror(fp)) {
      return lc_engine_set_transport_error(error, strerror(errno));
    }
    return lc_engine_set_transport_error(error,
                                         "failed to write temporary JSON file");
  }
  return 1;
}

int lc_lonejson_parse_file(FILE *fp, const lonejson_map *map, void *dst,
                           const lonejson_parse_options *options,
                           lc_error *error, const char *message) {
  lonejson_error lj_error;
  lonejson_status status;

  memset(&lj_error, 0, sizeof(lj_error));
  if (fp == NULL || map == NULL || dst == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        message != NULL ? message
                        : "lonejson parse requires file, map, and destination",
        NULL, NULL, NULL);
  }
  status = lonejson_parse_filep(map, dst, fp, options, &lj_error);
  return lc_lonejson_error_from_status(error, status, &lj_error, message);
}

int lc_lonejson_serialize_file(FILE *fp, const lonejson_map *map,
                               const void *src,
                               const lonejson_write_options *options,
                               lc_error *error, const char *message) {
  lonejson_error lj_error;
  lonejson_status status;

  memset(&lj_error, 0, sizeof(lj_error));
  if (fp == NULL || map == NULL || src == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        message != NULL ? message
                        : "lonejson serialize requires file, map, and source",
        NULL, NULL, NULL);
  }
  status = lonejson_serialize_filep(map, src, fp, options, &lj_error);
  return lc_lonejson_error_from_status(error, status, &lj_error, message);
}
