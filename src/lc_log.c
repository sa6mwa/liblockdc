#include "lc_log.h"

#include <pthread.h>
#include <string.h>

static pthread_once_t lc_log_noop_once = PTHREAD_ONCE_INIT;
static pslog_logger *lc_log_noop = NULL;

static void lc_log_init_noop(void) {
  pslog_config config;

  pslog_default_config(&config);
  config.min_level = PSLOG_LEVEL_DISABLED;
  lc_log_noop = pslog_new(&config);
}

pslog_logger *lc_log_noop_logger(void) {
  pthread_once(&lc_log_noop_once, lc_log_init_noop);
  return lc_log_noop;
}

pslog_logger *lc_log_client_logger(pslog_logger *base_logger,
                                   int disable_sys_field) {
  pslog_field field;

  if (base_logger == NULL) {
    base_logger = lc_log_noop_logger();
  }
  if (disable_sys_field) {
    return base_logger;
  }
  field = pslog_trusted_str("sys", "client.lockd");
  return pslog_with(base_logger, &field, 1U);
}

pslog_logger *lc_log_pouch_logger(pslog_logger *base_logger) {
  pslog_field field;

  if (base_logger == NULL) {
    base_logger = lc_log_noop_logger();
  }
  field = pslog_trusted_str("sys", "storage.pouch");
  return pslog_with(base_logger, &field, 1U);
}

static const char *lc_log_canonical_key(const char *key) {
  if (key == NULL) {
    return key;
  }
  if (strcmp(key, "namespace") == 0) {
    return "ns";
  }
  if (strcmp(key, "message_id") == 0) {
    return "msg_id";
  }
  if (strcmp(key, "head_message_id") == 0) {
    return "head_msg_id";
  }
  if (strcmp(key, "current_version") == 0) {
    return "cur_version";
  }
  if (strcmp(key, "current_etag") == 0) {
    return "cur_etag";
  }
  if (strcmp(key, "http_status") == 0) {
    return "status";
  }
  if (strcmp(key, "ttl_seconds") == 0) {
    return "ttl_s";
  }
  if (strcmp(key, "delay_seconds") == 0) {
    return "delay_s";
  }
  if (strcmp(key, "extend_by_seconds") == 0) {
    return "extend_s";
  }
  if (strcmp(key, "visibility_timeout_seconds") == 0) {
    return "visibility_s";
  }
  if (strcmp(key, "restart_in_ms") == 0) {
    return "restart_ms";
  }
  return key;
}

static void lc_log_emit(void (*emit)(pslog_logger *, const char *,
                                     const pslog_field *, size_t),
                        pslog_logger *logger, const char *msg,
                        const pslog_field *fields, size_t count) {
  if (logger == NULL || emit == NULL || msg == NULL) {
    return;
  }
  emit(logger, msg, fields, count);
}

void lc_log_trace(pslog_logger *logger, const char *msg,
                  const pslog_field *fields, size_t count) {
  lc_log_emit(pslog_trace, logger, msg, fields, count);
}

void lc_log_debug(pslog_logger *logger, const char *msg,
                  const pslog_field *fields, size_t count) {
  lc_log_emit(pslog_debug, logger, msg, fields, count);
}

void lc_log_info(pslog_logger *logger, const char *msg,
                 const pslog_field *fields, size_t count) {
  lc_log_emit(pslog_info, logger, msg, fields, count);
}

void lc_log_warn(pslog_logger *logger, const char *msg,
                 const pslog_field *fields, size_t count) {
  lc_log_emit(pslog_warn, logger, msg, fields, count);
}

void lc_log_error(pslog_logger *logger, const char *msg,
                  const pslog_field *fields, size_t count) {
  lc_log_emit(pslog_error, logger, msg, fields, count);
}

pslog_field lc_log_str_field(const char *key, const char *value) {
  key = lc_log_canonical_key(key);
  if (value == NULL) {
    return pslog_null(key);
  }
  return pslog_str(key, value);
}

pslog_field lc_log_bool_field(const char *key, int value) {
  key = lc_log_canonical_key(key);
  return pslog_bool(key, value);
}

pslog_field lc_log_i64_field(const char *key, long value) {
  key = lc_log_canonical_key(key);
  return pslog_i64(key, (pslog_int64)value);
}

pslog_field lc_log_u64_field(const char *key, size_t value) {
  key = lc_log_canonical_key(key);
  return pslog_u64(key, (pslog_uint64)value);
}

pslog_field lc_log_error_field(const char *key, const lc_error *error) {
  key = lc_log_canonical_key(key);
  if (error == NULL || error->message == NULL) {
    return pslog_null(key);
  }
  return pslog_str(key, error->message);
}

pslog_field lc_log_http_status_field(const lc_error *error) {
  if (error == NULL) {
    return pslog_null("status");
  }
  return pslog_i64("status", (pslog_int64)error->http_status);
}

pslog_field lc_log_code_field(const lc_error *error) {
  if (error == NULL) {
    return pslog_null("code");
  }
  return pslog_i64("code", (pslog_int64)error->code);
}
