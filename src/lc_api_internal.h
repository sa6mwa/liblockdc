#ifndef LC_API_INTERNAL_H
#define LC_API_INTERNAL_H

/*
 * Private SDK implementation details.
 *
 * Public callers should only include include/lc/lc.h. The structs and helpers
 * in this header are internal to the shipped HTTP/curl/lonejson implementation
 * of that public contract.
 */

#ifndef LONEJSON_WITH_CURL
#define LONEJSON_WITH_CURL
#endif
#include "lc/lc.h"
#include "lc_engine_api.h"

#include <pslog.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct lc_stream_pipe lc_stream_pipe;

typedef struct lc_client_handle lc_client_handle;
typedef struct lc_lease_handle lc_lease_handle;
typedef struct lc_message_handle lc_message_handle;
typedef struct lc_consumer_service_handle lc_consumer_service_handle;

struct lc_client_handle {
  lc_client pub;
  lc_engine_client *engine;
  char **endpoints;
  size_t endpoint_count;
  char *unix_socket_path;
  char *client_bundle_path;
  char *default_namespace;
  long timeout_ms;
  int disable_mtls;
  int insecure_skip_verify;
  int prefer_http_2;
  size_t http_json_response_limit_bytes;
  int disable_logger_sys_field;
  pslog_logger *base_logger;
  pslog_logger *logger;
  lc_allocator allocator;
};

struct lc_lease_handle {
  lc_lease pub;
  lc_client_handle *client;
  char *namespace_name;
  char *key;
  char *owner;
  char *lease_id;
  char *txn_id;
  long fencing_token;
  long version;
  long lease_expires_at_unix;
  char *state_etag;
  char *queue_state_etag;
  int has_query_hidden;
  int query_hidden;
};

struct lc_message_handle {
  lc_message pub;
  lc_client_handle *client;
  char *namespace_name;
  char *queue;
  char *message_id;
  int attempts;
  int max_attempts;
  int failure_attempts;
  long not_visible_until_unix;
  long visibility_timeout_seconds;
  char *payload_content_type;
  char *correlation_id;
  char *lease_id;
  long lease_expires_at_unix;
  long fencing_token;
  char *txn_id;
  char *meta_etag;
  char *next_cursor;
  lc_source *payload;
  int *terminal_flag;
  lc_lease *state_lease;
  char *state_etag;
  char *state_lease_id;
  long state_lease_expires_at_unix;
  long state_fencing_token;
  char *state_txn_id;
};

typedef struct lc_write_bridge {
  lc_sink *sink;
} lc_write_bridge;

typedef struct lc_read_bridge {
  lc_source *source;
} lc_read_bridge;

typedef struct lc_watch_bridge {
  const lc_watch_handler *handler;
} lc_watch_bridge;

typedef struct lc_single_delivery_bridge {
  lc_client_handle *client;
  lc_message **out;
  lc_dequeue_batch_res *batch;
  lc_error *error;
  lc_engine_dequeue_response meta;
  lc_source *payload;
  lc_stream_pipe *pipe;
  int mode_batch;
} lc_single_delivery_bridge;

typedef struct lc_subscribe_bridge {
  lc_client_handle *client;
  const lc_consumer *consumer;
  lc_error *error;
  lc_engine_dequeue_response meta;
  lc_stream_pipe *pipe;
  pthread_t handler_thread;
  int handler_thread_started;
  int handler_rc;
  int terminal;
  lc_message *message;
} lc_subscribe_bridge;

int lc_error_set(lc_error *error, int code, long http_status,
                 const char *message, const char *detail,
                 const char *server_code, const char *correlation_id);
int lc_error_from_engine(lc_error *error, lc_engine_error *engine);
const char *lc_nack_intent_to_string(lc_nack_intent intent);
int lc_nack_intent_to_wire_string(lc_nack_intent intent, const char **out,
                                  lc_error *error);
void *lc_alloc_with_allocator(const lc_allocator *allocator, size_t size);
void *lc_calloc_with_allocator(const lc_allocator *allocator, size_t count,
                               size_t size);
void *lc_realloc_with_allocator(const lc_allocator *allocator, void *ptr,
                                size_t size);
void lc_free_with_allocator(const lc_allocator *allocator, void *ptr);
char *lc_strdup_with_allocator(const lc_allocator *allocator,
                               const char *value);
char *lc_dup_bytes_with_allocator(const lc_allocator *allocator,
                                  const void *bytes, size_t length);
void *lc_client_alloc(lc_client_handle *client, size_t size);
void *lc_client_calloc(lc_client_handle *client, size_t count, size_t size);
void *lc_client_realloc(lc_client_handle *client, void *ptr, size_t size);
void lc_client_free(lc_client_handle *client, void *ptr);
char *lc_client_strdup(lc_client_handle *client, const char *value);
char *lc_strdup_local(const char *value);
char *lc_dup_bytes_as_text(const void *bytes, size_t length);
void lc_attachment_info_copy(lc_attachment_info *dst,
                             const lc_engine_attachment_info *src);
size_t lc_engine_read_bridge(void *context, void *buffer, size_t count,
                             lc_engine_error *error);
int lc_engine_reset_bridge(void *context, lc_engine_error *error);
int lc_engine_write_bridge(void *context, const void *bytes, size_t count,
                           lc_engine_error *error);
lc_source *lc_source_from_open_file(FILE *fp, int close_file);
int lc_stream_pipe_open(size_t capacity, const lc_allocator *allocator,
                        lc_source **out, lc_stream_pipe **pipe,
                        lc_error *error);
int lc_stream_pipe_write(lc_stream_pipe *pipe, const void *bytes, size_t count,
                         lc_error *error);
void lc_stream_pipe_finish(lc_stream_pipe *pipe);
void lc_stream_pipe_fail(lc_stream_pipe *pipe, int code, const char *message);
lc_lease *lc_lease_new(lc_client_handle *client, const char *namespace_name,
                       const char *key, const char *owner, const char *lease_id,
                       const char *txn_id, long fencing_token, long version,
                       const char *state_etag, const char *queue_state_etag);
lc_message *lc_message_new(lc_client_handle *client,
                           const lc_engine_dequeue_response *engine,
                           lc_source *payload, int *terminal_flag);

int lc_client_acquire_method(lc_client *self, const lc_acquire_req *req,
                             lc_lease **out, lc_error *error);
int lc_client_describe_method(lc_client *self, const lc_describe_req *req,
                              lc_describe_res *out, lc_error *error);
int lc_client_get_method(lc_client *self, const char *key,
                         const lc_get_opts *opts, lc_sink *dst, lc_get_res *out,
                         lc_error *error);
int lc_client_load_method(lc_client *self, const char *key,
                          const lonejson_map *map, void *dst,
                          const lonejson_parse_options *parse_options,
                          const lc_get_opts *opts, lc_get_res *out,
                          lc_error *error);
int lc_client_update_method(lc_client *self, const lc_update_req *req,
                            lc_source *src, lc_update_res *out,
                            lc_error *error);
int lc_client_mutate_method(lc_client *self, const lc_mutate_op *req,
                            lc_mutate_res *out, lc_error *error);
int lc_client_metadata_method(lc_client *self, const lc_metadata_op *req,
                              lc_metadata_res *out, lc_error *error);
int lc_client_remove_method(lc_client *self, const lc_remove_op *req,
                            lc_remove_res *out, lc_error *error);
int lc_client_keepalive_method(lc_client *self, const lc_keepalive_op *req,
                               lc_keepalive_res *out, lc_error *error);
int lc_client_release_method(lc_client *self, const lc_release_op *req,
                             lc_release_res *out, lc_error *error);
int lc_client_attach_method(lc_client *self, const lc_attach_op *req,
                            lc_source *src, lc_attach_res *out,
                            lc_error *error);
int lc_client_list_attachments_method(lc_client *self,
                                      const lc_attachment_list_req *req,
                                      lc_attachment_list *out, lc_error *error);
int lc_client_get_attachment_method(lc_client *self,
                                    const lc_attachment_get_op *req,
                                    lc_sink *dst, lc_attachment_get_res *out,
                                    lc_error *error);
int lc_client_delete_attachment_method(lc_client *self,
                                       const lc_attachment_delete_op *req,
                                       int *deleted, lc_error *error);
int lc_client_delete_all_attachments_method(
    lc_client *self, const lc_attachment_delete_all_op *req, int *deleted_count,
    lc_error *error);
int lc_client_queue_stats_method(lc_client *self, const lc_queue_stats_req *req,
                                 lc_queue_stats_res *out, lc_error *error);
int lc_client_queue_ack_method(lc_client *self, const lc_ack_op *req,
                               lc_ack_res *out, lc_error *error);
int lc_client_queue_nack_method(lc_client *self, const lc_nack_op *req,
                                lc_nack_res *out, lc_error *error);
int lc_client_queue_extend_method(lc_client *self, const lc_extend_op *req,
                                  lc_extend_res *out, lc_error *error);
int lc_client_query_method(lc_client *self, const lc_query_req *req,
                           lc_sink *dst, lc_query_res *out, lc_error *error);
int lc_client_get_namespace_config_method(lc_client *self,
                                          const lc_namespace_config_req *req,
                                          lc_namespace_config_res *out,
                                          lc_error *error);
int lc_client_update_namespace_config_method(lc_client *self,
                                             const lc_namespace_config_req *req,
                                             lc_namespace_config_res *out,
                                             lc_error *error);
int lc_client_flush_index_method(lc_client *self, const lc_index_flush_req *req,
                                 lc_index_flush_res *out, lc_error *error);
int lc_client_txn_replay_method(lc_client *self, const lc_txn_replay_req *req,
                                lc_txn_replay_res *out, lc_error *error);
int lc_client_txn_prepare_method(lc_client *self,
                                 const lc_txn_decision_req *req,
                                 lc_txn_decision_res *out, lc_error *error);
int lc_client_txn_commit_method(lc_client *self, const lc_txn_decision_req *req,
                                lc_txn_decision_res *out, lc_error *error);
int lc_client_txn_rollback_method(lc_client *self,
                                  const lc_txn_decision_req *req,
                                  lc_txn_decision_res *out, lc_error *error);
int lc_client_tc_lease_acquire_method(lc_client *self,
                                      const lc_tc_lease_acquire_req *req,
                                      lc_tc_lease_acquire_res *out,
                                      lc_error *error);
int lc_client_tc_lease_renew_method(lc_client *self,
                                    const lc_tc_lease_renew_req *req,
                                    lc_tc_lease_renew_res *out,
                                    lc_error *error);
int lc_client_tc_lease_release_method(lc_client *self,
                                      const lc_tc_lease_release_req *req,
                                      lc_tc_lease_release_res *out,
                                      lc_error *error);
int lc_client_tc_leader_method(lc_client *self, lc_tc_leader_res *out,
                               lc_error *error);
int lc_client_tc_cluster_announce_method(lc_client *self,
                                         const lc_tc_cluster_announce_req *req,
                                         lc_tc_cluster_res *out,
                                         lc_error *error);
int lc_client_tc_cluster_leave_method(lc_client *self, lc_tc_cluster_res *out,
                                      lc_error *error);
int lc_client_tc_cluster_list_method(lc_client *self, lc_tc_cluster_res *out,
                                     lc_error *error);
int lc_client_tc_rm_register_method(lc_client *self,
                                    const lc_tc_rm_register_req *req,
                                    lc_tc_rm_res *out, lc_error *error);
int lc_client_tc_rm_unregister_method(lc_client *self,
                                      const lc_tc_rm_unregister_req *req,
                                      lc_tc_rm_res *out, lc_error *error);
int lc_client_tc_rm_list_method(lc_client *self, lc_tc_rm_list_res *out,
                                lc_error *error);
int lc_client_enqueue_method(lc_client *self, const lc_enqueue_req *req,
                             lc_source *src, lc_enqueue_res *out,
                             lc_error *error);
int lc_client_dequeue_method(lc_client *self, const lc_dequeue_req *req,
                             lc_message **out, lc_error *error);
int lc_client_dequeue_batch_method(lc_client *self, const lc_dequeue_req *req,
                                   lc_dequeue_batch_res *out, lc_error *error);
int lc_client_dequeue_with_state_method(lc_client *self,
                                        const lc_dequeue_req *req,
                                        lc_message **out, lc_error *error);
int lc_client_subscribe_method(lc_client *self, const lc_dequeue_req *req,
                               const lc_consumer *consumer, lc_error *error);
int lc_client_subscribe_with_state_method(lc_client *self,
                                          const lc_dequeue_req *req,
                                          const lc_consumer *consumer,
                                          lc_error *error);
int lc_client_new_consumer_service_method(
    lc_client *self, const lc_consumer_service_config *config,
    lc_consumer_service **out, lc_error *error);
int lc_client_watch_queue_method(lc_client *self, const lc_watch_queue_req *req,
                                 const lc_watch_handler *handler,
                                 lc_error *error);
void lc_client_close_method(lc_client *self);

int lc_lease_describe_method(lc_lease *self, lc_error *error);
int lc_lease_get_method(lc_lease *self, lc_sink *dst, const lc_get_opts *opts,
                        lc_get_res *out, lc_error *error);
int lc_lease_load_method(lc_lease *self, const lonejson_map *map, void *dst,
                         const lonejson_parse_options *parse_options,
                         const lc_get_opts *opts, lc_get_res *out,
                         lc_error *error);
int lc_lease_save_method(lc_lease *self, const lonejson_map *map,
                         const void *src,
                         const lonejson_write_options *write_options,
                         lc_error *error);
int lc_lease_update_method(lc_lease *self, lc_source *src,
                           const lc_update_opts *opts, lc_error *error);
int lc_lease_mutate_method(lc_lease *self, const lc_mutate_req *req,
                           lc_error *error);
int lc_lease_mutate_local_method(lc_lease *self, const lc_mutate_local_req *req,
                                 lc_error *error);
int lc_lease_metadata_method(lc_lease *self, const lc_metadata_req *req,
                             lc_error *error);
int lc_lease_remove_method(lc_lease *self, const lc_remove_req *req,
                           lc_error *error);
int lc_lease_keepalive_method(lc_lease *self, const lc_keepalive_req *req,
                              lc_error *error);
int lc_lease_release_method(lc_lease *self, const lc_release_req *req,
                            lc_error *error);
int lc_lease_attach_method(lc_lease *self, const lc_attach_req *req,
                           lc_source *src, lc_attach_res *out, lc_error *error);
int lc_lease_list_attachments_method(lc_lease *self, lc_attachment_list *out,
                                     lc_error *error);
int lc_lease_get_attachment_method(lc_lease *self,
                                   const lc_attachment_get_req *req,
                                   lc_sink *dst, lc_attachment_get_res *out,
                                   lc_error *error);
int lc_lease_delete_attachment_method(lc_lease *self,
                                      const lc_attachment_selector *selector,
                                      int *deleted, lc_error *error);
int lc_lease_delete_all_attachments_method(lc_lease *self, int *deleted_count,
                                           lc_error *error);
void lc_lease_close_method(lc_lease *self);

int lc_message_ack_method(lc_message *self, lc_error *error);
int lc_message_nack_method(lc_message *self, const lc_nack_req *req,
                           lc_error *error);
int lc_message_extend_method(lc_message *self, const lc_extend_req *req,
                             lc_error *error);
lc_lease *lc_message_state_method(lc_message *self);
lc_source *lc_message_payload_reader_method(lc_message *self);
int lc_message_rewind_payload_method(lc_message *self, lc_error *error);
int lc_message_write_payload_method(lc_message *self, lc_sink *dst,
                                    size_t *written, lc_error *error);

int lc_lonejson_error_from_status(lc_error *error, lonejson_status status,
                                  const lonejson_error *lj_error,
                                  const char *message);
int lc_engine_file_write_callback(void *context, const void *bytes,
                                  size_t count, lc_engine_error *error);
int lc_lonejson_parse_file(FILE *fp, const lonejson_map *map, void *dst,
                           const lonejson_parse_options *options,
                           lc_error *error, const char *message);
int lc_lonejson_serialize_file(FILE *fp, const lonejson_map *map,
                               const void *src,
                               const lonejson_write_options *options,
                               lc_error *error, const char *message);
void lc_message_close_method(lc_message *self);

int lc_consumer_service_run_method(lc_consumer_service *self, lc_error *error);
int lc_consumer_service_start_method(lc_consumer_service *self,
                                     lc_error *error);
int lc_consumer_service_stop_method(lc_consumer_service *self);
int lc_consumer_service_wait_method(lc_consumer_service *self, lc_error *error);
void lc_consumer_service_close_method(lc_consumer_service *self);

#endif
