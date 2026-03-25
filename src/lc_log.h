#ifndef LC_LOG_H
#define LC_LOG_H

#include "lc/lc.h"

#include <pslog.h>
#include <stddef.h>

pslog_logger *lc_log_client_logger(pslog_logger *base_logger,
                                   int disable_sys_field);
pslog_logger *lc_log_noop_logger(void);

void lc_log_trace(pslog_logger *logger, const char *msg,
                  const pslog_field *fields, size_t count);
void lc_log_debug(pslog_logger *logger, const char *msg,
                  const pslog_field *fields, size_t count);
void lc_log_info(pslog_logger *logger, const char *msg,
                 const pslog_field *fields, size_t count);
void lc_log_warn(pslog_logger *logger, const char *msg,
                 const pslog_field *fields, size_t count);
void lc_log_error(pslog_logger *logger, const char *msg,
                  const pslog_field *fields, size_t count);

pslog_field lc_log_str_field(const char *key, const char *value);
pslog_field lc_log_bool_field(const char *key, int value);
pslog_field lc_log_i64_field(const char *key, long value);
pslog_field lc_log_u64_field(const char *key, size_t value);
pslog_field lc_log_error_field(const char *key, const lc_error *error);
pslog_field lc_log_http_status_field(const lc_error *error);
pslog_field lc_log_code_field(const lc_error *error);

#endif
