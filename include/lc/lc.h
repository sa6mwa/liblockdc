#ifndef LC_LOCKDC_H
#define LC_LOCKDC_H

#include <lc/version.h>
#include <pslog.h>
#include <stddef.h>

/** Opaque client handle. */
typedef struct lc_client lc_client;
/** Opaque lease/state handle returned from acquire and message state flows. */
typedef struct lc_lease lc_lease;
/** Opaque queue message handle returned from dequeue and subscription flows. */
typedef struct lc_message lc_message;
/** Opaque managed queue consumer service. */
typedef struct lc_consumer_service lc_consumer_service;
/** Opaque JSON input stream used for large state updates. */
typedef struct lc_json lc_json;
/** Opaque byte source used for uploads and streamed request bodies. */
typedef struct lc_source lc_source;
/** Opaque byte sink used for downloads and streamed response bodies. */
typedef struct lc_sink lc_sink;

/**
 * Structured error returned by all public operations.
 *
 * The library allocates any string members it populates. Release them with
 * `lc_error_cleanup()` when the error is no longer needed.
 */
typedef struct lc_error {
  int code;
  long http_status;
  char *message;
  char *detail;
  char *server_code;
  char *correlation_id;
} lc_error;

/** Custom allocation hook for malloc-style allocation. */
typedef void *(*lc_malloc_fn)(void *context, size_t size);
/** Custom allocation hook for realloc-style allocation. */
typedef void *(*lc_realloc_fn)(void *context, void *ptr, size_t size);
/** Custom allocation hook for freeing memory returned by the allocator hooks.
 */
typedef void (*lc_free_fn)(void *context, void *ptr);

/**
 * Allocator override used by the client and all derived handles.
 *
 * Leave zeroed to use the platform allocator.
 */
typedef struct lc_allocator {
  lc_malloc_fn malloc_fn;
  lc_realloc_fn realloc_fn;
  lc_free_fn free_fn;
  void *context;
} lc_allocator;

/**
 * Client construction settings.
 *
 * Use `lc_client_config_init()` before overriding individual fields.
 * Set either `endpoints` for TCP/TLS operation or `unix_socket_path`
 * for Unix-domain-socket transport.
 */
typedef struct lc_client_config {
  /** HTTPS base URLs tried in order for TCP/TLS transport. */
  const char *const *endpoints;
  /** Number of entries in `endpoints`. */
  size_t endpoint_count;
  /** Unix-domain socket path used instead of `endpoints` when set. */
  const char *unix_socket_path;
  /** Path to the combined CA/client-cert/private-key PEM bundle. */
  const char *client_bundle_path;
  /** Namespace used when a request leaves `namespace_name` unset. */
  const char *default_namespace;
  /** Whole-request timeout in milliseconds. */
  long timeout_ms;
  /** Disables mTLS client authentication when the server allows it. */
  int disable_mtls;
  /** Skips peer verification. Intended for local dev and test only. */
  int insecure_skip_verify;
  /** Prefers HTTP/2 when the endpoint and libcurl build support it. */
  int prefer_http_2;
  /** Borrowed client logger used for SDK diagnostics. Defaults to a no-op
   * logger. */
  pslog_logger *logger;
  /** Disables the automatic `sys=client.lockd` logger field derived by the
   * SDK when non-zero. */
  int disable_logger_sys_field;
  /** Custom allocator hooks inherited by all derived handles and buffers. */
  lc_allocator allocator;
} lc_client_config;

/** Public status codes returned by all API entry points. */
enum {
  LC_OK = 0,
  LC_ERR_INVALID = 1,
  LC_ERR_NOMEM = 2,
  LC_ERR_TRANSPORT = 3,
  LC_ERR_PROTOCOL = 4,
  LC_ERR_SERVER = 5
};

/**
 * Rewindable input stream used for uploads and generic byte transport.
 *
 * The source owns `impl` and releases it from `close()`.
 */
struct lc_source {
  size_t (*read)(lc_source *self, void *buffer, size_t count, lc_error *error);
  int (*reset)(lc_source *self, lc_error *error);
  void (*close)(lc_source *self);
  void *impl;
};

/**
 * Output stream used for downloads and generic byte transport.
 *
 * The sink owns `impl` and releases it from `close()`.
 */
struct lc_sink {
  int (*write)(lc_sink *self, const void *bytes, size_t count, lc_error *error);
  void (*close)(lc_sink *self);
  void *impl;
};

/**
 * Rewindable JSON source used for streamed state updates.
 *
 * This is the preferred input type for large JSON documents that must not be
 * fully materialized in memory.
 */
struct lc_json {
  size_t (*read)(lc_json *self, void *buffer, size_t count, lc_error *error);
  int (*reset)(lc_json *self, lc_error *error);
  void (*close)(lc_json *self);
  void *impl;
};

/** Callback used to resolve a file-backed local-mutate input into a new source.
 */
typedef int (*lc_file_value_open_fn)(void *context, const char *resolved_path,
                                     lc_source **out, lc_error *error);

/** Optional override used by local mutate to open file-backed values. */
typedef struct lc_file_value_resolver {
  lc_file_value_open_fn open;
  void *context;
} lc_file_value_resolver;

/** Request used to acquire a new lease. */
typedef struct lc_acquire_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *namespace_name;
  /** Lease key to acquire. */
  const char *key;
  /** Logical owner identifier recorded by the server. */
  const char *owner;
  /** Requested lease TTL in seconds. */
  long ttl_seconds;
  /** Optional server-side blocking acquire wait in seconds. */
  long block_seconds;
  /** When non-zero, acquisition succeeds only if the key does not yet exist. */
  int if_not_exists;
  /** Optional transaction identifier to bind the acquire into. */
  const char *txn_id;
} lc_acquire_req;

/** Result metadata returned by a successful lease acquisition. */
typedef struct lc_acquire_res {
  long version;
  char *state_etag;
  long fencing_token;
  char *correlation_id;
} lc_acquire_res;

/** Stable lease identity used by client-level operations on an existing lease.
 */
typedef struct lc_lease_ref {
  /** Namespace of the existing lease. */
  const char *namespace_name;
  /** Key of the existing lease. */
  const char *key;
  /** Server-issued lease identifier. */
  const char *lease_id;
  /** Transaction identifier associated with the lease, if any. */
  const char *txn_id;
  /** Current fencing token for optimistic concurrency. */
  long fencing_token;
} lc_lease_ref;

/** Request used to describe a lease or state object. */
typedef struct lc_describe_req {
  const char *namespace_name;
  const char *key;
} lc_describe_req;

/** Lease description returned by describe operations. */
typedef struct lc_describe_res {
  char *namespace_name;
  char *key;
  char *owner;
  long version;
  char *lease_id;
  long lease_expires_at_unix;
  long fencing_token;
  char *txn_id;
  char *state_etag;
  char *public_state_etag;
  int has_query_hidden;
  int query_hidden;
  char *correlation_id;
} lc_describe_res;

/** Optional flags that control state reads. */
typedef struct lc_get_opts {
  /** Reads the public view instead of the private lease-bound state. */
  int public_read;
} lc_get_opts;

/** Metadata returned by streamed or buffered state reads. */
typedef struct lc_get_res {
  int no_content;
  char *content_type;
  char *etag;
  long version;
  long fencing_token;
  char *correlation_id;
} lc_get_res;

/** Optional controls for streamed JSON state updates. */
typedef struct lc_update_opts {
  /** Optional state etag precondition for optimistic concurrency. */
  const char *if_state_etag;
  /** Optional version precondition for optimistic concurrency. */
  long if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
  /** Content type sent with the JSON state body. */
  const char *content_type;
} lc_update_opts;

/** Client-level update operation for an existing lease reference. */
typedef struct lc_update_req {
  /** Existing lease identity to update. */
  lc_lease_ref lease;
  /** Optional state etag precondition for optimistic concurrency. */
  const char *if_state_etag;
  /** Optional version precondition for optimistic concurrency. */
  long if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
  /** Content type sent with the JSON state body. */
  const char *content_type;
} lc_update_req;

/** Metadata returned by a successful state update. */
typedef struct lc_update_res {
  long new_version;
  char *new_state_etag;
  long bytes;
  char *correlation_id;
} lc_update_res;

/** Request that applies one or more server-side state mutations. */
typedef struct lc_mutate_req {
  /** JSON mutation expressions evaluated in order by the server. */
  const char *const *mutations;
  /** Number of entries in `mutations`. */
  size_t mutation_count;
  /** Optional state etag precondition for optimistic concurrency. */
  const char *if_state_etag;
  /** Optional version precondition for optimistic concurrency. */
  long if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
} lc_mutate_req;

/**
 * Request used by `mutate_local()` to stream a state document through the
 * local mutate engine and then upload the staged JSON result back to lockd.
 */
typedef struct lc_mutate_local_req {
  /** JSON mutation expressions evaluated in order by the local mutate engine.
   */
  const char *const *mutations;
  /** Number of entries in `mutations`. */
  size_t mutation_count;
  /** When non-zero, do not default `if_state_etag`/`if_version` from `get()`.
   */
  int disable_fetched_cas;
  /** Base directory used to resolve relative `file:`/`textfile:` paths. */
  const char *file_value_base_dir;
  /** Optional file opener override for file-backed mutators. */
  const lc_file_value_resolver *file_value_resolver;
  /** Optional explicit update preconditions and content type overrides. */
  lc_update_opts update;
} lc_mutate_local_req;

/** Client-level mutate operation for an existing lease reference. */
typedef struct lc_mutate_op {
  lc_lease_ref lease;
  const char *const *mutations;
  size_t mutation_count;
  const char *if_state_etag;
  long if_version;
  int has_if_version;
} lc_mutate_op;

/** Metadata returned by a successful mutate operation. */
typedef struct lc_mutate_res {
  long new_version;
  char *new_state_etag;
  long bytes;
  char *correlation_id;
} lc_mutate_res;

/** Request used to update lease metadata without replacing state bytes. */
typedef struct lc_metadata_req {
  /** Set to non-zero when `query_hidden` should be updated. */
  int has_query_hidden;
  /** Whether the state should be hidden from normal query results. */
  int query_hidden;
  /** Optional version precondition for optimistic concurrency. */
  long if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
} lc_metadata_req;

/** Client-level metadata update operation for an existing lease reference. */
typedef struct lc_metadata_op {
  lc_lease_ref lease;
  int has_query_hidden;
  int query_hidden;
  long if_version;
  int has_if_version;
} lc_metadata_op;

/** Metadata returned by a successful metadata update. */
typedef struct lc_metadata_res {
  char *namespace_name;
  char *key;
  long version;
  int has_query_hidden;
  int query_hidden;
  char *correlation_id;
} lc_metadata_res;

/** Request used to remove state bytes while keeping the lease. */
typedef struct lc_remove_req {
  /** Optional state etag precondition for optimistic concurrency. */
  const char *if_state_etag;
  /** Optional version precondition for optimistic concurrency. */
  long if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
} lc_remove_req;

/** Client-level remove operation for an existing lease reference. */
typedef struct lc_remove_op {
  lc_lease_ref lease;
  const char *if_state_etag;
  long if_version;
  int has_if_version;
} lc_remove_op;

/** Result returned by a remove operation. */
typedef struct lc_remove_res {
  int removed;
  long new_version;
  char *correlation_id;
} lc_remove_res;

/** Request used to extend an existing lease. */
typedef struct lc_keepalive_req {
  /** New TTL in seconds for the renewed lease. */
  long ttl_seconds;
} lc_keepalive_req;

/** Client-level keepalive operation for an existing lease reference. */
typedef struct lc_keepalive_op {
  lc_lease_ref lease;
  long ttl_seconds;
} lc_keepalive_op;

/** Result returned by a successful keepalive operation. */
typedef struct lc_keepalive_res {
  long lease_expires_at_unix;
  long version;
  char *state_etag;
  char *correlation_id;
} lc_keepalive_res;

/** Request used to release a lease, optionally rolling back transactional
 * state. */
typedef struct lc_release_req {
  /** Rolls transactional state back before releasing when non-zero. */
  int rollback;
} lc_release_req;

/** Client-level release operation for an existing lease reference. */
typedef struct lc_release_op {
  lc_lease_ref lease;
  int rollback;
} lc_release_op;

/** Result returned by a release operation. */
typedef struct lc_release_res {
  int released;
  char *correlation_id;
} lc_release_res;

/** Streamed query request. Query result rows are written into a sink. */
typedef struct lc_query_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *namespace_name;
  /** JSON selector expression used by the query engine. */
  const char *selector_json;
  /** Maximum number of rows to return in this page. */
  long limit;
  /** Cursor returned by a previous query page, or `NULL` for a fresh query. */
  const char *cursor;
  /** Optional JSON field-selection expression. */
  const char *fields_json;
  /** Server-specific query return mode. */
  const char *return_mode;
} lc_query_req;

/** Query metadata returned alongside streamed query results. */
typedef struct lc_query_res {
  char *cursor;
  char *return_mode;
  unsigned long index_seq;
  char *correlation_id;
} lc_query_res;

/** Generic owned string list used by several management responses. */
typedef struct lc_string_list {
  char **items;
  size_t count;
} lc_string_list;

/** Request used to read or update namespace configuration. */
typedef struct lc_namespace_config_req {
  const char *namespace_name;
  const char *preferred_engine;
  const char *fallback_engine;
} lc_namespace_config_req;

/** Namespace configuration returned by get or update operations. */
typedef struct lc_namespace_config_res {
  char *namespace_name;
  char *preferred_engine;
  char *fallback_engine;
  char *correlation_id;
} lc_namespace_config_res;

/** Request used to flush an index for a namespace. */
typedef struct lc_index_flush_req {
  const char *namespace_name;
  const char *mode;
} lc_index_flush_req;

/** Result returned by an index flush request. */
typedef struct lc_index_flush_res {
  char *namespace_name;
  char *mode;
  char *flush_id;
  int accepted;
  int flushed;
  int pending;
  unsigned long index_seq;
  char *correlation_id;
} lc_index_flush_res;

/** Transaction participant identity used by TC prepare/commit/rollback
 * operations. */
typedef struct lc_txn_participant {
  const char *namespace_name;
  const char *key;
  const char *backend_hash;
} lc_txn_participant;

/** Request used to replay transaction coordinator state for a transaction id.
 */
typedef struct lc_txn_replay_req {
  const char *txn_id;
} lc_txn_replay_req;

/** Result returned by transaction replay. */
typedef struct lc_txn_replay_res {
  char *txn_id;
  char *state;
  char *correlation_id;
} lc_txn_replay_res;

/** Request used for TC prepare, commit, and rollback decisions. */
typedef struct lc_txn_decision_req {
  const char *txn_id;
  const lc_txn_participant *participants;
  size_t participant_count;
  long expires_at_unix;
  unsigned long tc_term;
  const char *target_backend_hash;
} lc_txn_decision_req;

/** Result returned by TC prepare, commit, or rollback. */
typedef struct lc_txn_decision_res {
  char *txn_id;
  char *state;
  char *correlation_id;
} lc_txn_decision_res;

/** Request used to acquire the TC leader lease. */
typedef struct lc_tc_lease_acquire_req {
  const char *candidate_id;
  const char *candidate_endpoint;
  unsigned long term;
  long ttl_ms;
} lc_tc_lease_acquire_req;

/** Result returned by a TC leader lease acquisition attempt. */
typedef struct lc_tc_lease_acquire_res {
  int granted;
  char *leader_id;
  char *leader_endpoint;
  unsigned long term;
  long expires_at_unix;
  char *correlation_id;
} lc_tc_lease_acquire_res;

/** Request used to renew the TC leader lease. */
typedef struct lc_tc_lease_renew_req {
  const char *leader_id;
  unsigned long term;
  long ttl_ms;
} lc_tc_lease_renew_req;

/** Result returned by TC leader lease renewal. */
typedef struct lc_tc_lease_renew_res {
  int renewed;
  char *leader_id;
  char *leader_endpoint;
  unsigned long term;
  long expires_at_unix;
  char *correlation_id;
} lc_tc_lease_renew_res;

/** Request used to release the TC leader lease. */
typedef struct lc_tc_lease_release_req {
  const char *leader_id;
  unsigned long term;
} lc_tc_lease_release_req;

/** Result returned by TC leader lease release. */
typedef struct lc_tc_lease_release_res {
  int released;
  char *correlation_id;
} lc_tc_lease_release_res;

/** Snapshot of the current transaction coordinator leader. */
typedef struct lc_tc_leader_res {
  char *leader_id;
  char *leader_endpoint;
  unsigned long term;
  long expires_at_unix;
  char *correlation_id;
} lc_tc_leader_res;

/** Request used to announce the current node into the TC cluster. */
typedef struct lc_tc_cluster_announce_req {
  const char *self_endpoint;
} lc_tc_cluster_announce_req;

/** Cluster membership state returned by TC cluster operations. */
typedef struct lc_tc_cluster_res {
  lc_string_list endpoints;
  long updated_at_unix;
  long expires_at_unix;
  char *correlation_id;
} lc_tc_cluster_res;

/** Request used to register a resource manager endpoint for a backend hash. */
typedef struct lc_tc_rm_register_req {
  const char *backend_hash;
  const char *endpoint;
} lc_tc_rm_register_req;

/** Request used to unregister a resource manager endpoint for a backend hash.
 */
typedef struct lc_tc_rm_unregister_req {
  const char *backend_hash;
  const char *endpoint;
} lc_tc_rm_unregister_req;

/** Result returned by TC RM register and unregister operations. */
typedef struct lc_tc_rm_res {
  char *backend_hash;
  lc_string_list endpoints;
  long updated_at_unix;
  char *correlation_id;
} lc_tc_rm_res;

/** Single TC RM backend entry returned by list operations. */
typedef struct lc_tc_rm_backend {
  char *backend_hash;
  lc_string_list endpoints;
  long updated_at_unix;
} lc_tc_rm_backend;

/** Full TC RM backend listing. */
typedef struct lc_tc_rm_list_res {
  lc_tc_rm_backend *backends;
  size_t backend_count;
  long updated_at_unix;
  char *correlation_id;
} lc_tc_rm_list_res;

typedef struct lc_enqueue_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *namespace_name;
  /** Queue name. */
  const char *queue;
  /** Initial delivery delay in seconds. */
  long delay_seconds;
  /** Visibility timeout granted to the consumer on dequeue. */
  long visibility_timeout_seconds;
  /** Message TTL in seconds. */
  long ttl_seconds;
  /** Maximum delivery attempts before the server gives up. */
  int max_attempts;
  /** Payload content type recorded with the message. */
  const char *content_type;
} lc_enqueue_req;

/** Result returned by a successful queue enqueue operation. */
typedef struct lc_enqueue_res {
  char *namespace_name;
  char *queue;
  char *message_id;
  int attempts;
  int max_attempts;
  int failure_attempts;
  long not_visible_until_unix;
  long visibility_timeout_seconds;
  long payload_bytes;
  char *correlation_id;
} lc_enqueue_res;

/** Request used to dequeue queue messages. */
typedef struct lc_dequeue_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *namespace_name;
  /** Queue name. */
  const char *queue;
  /** Logical consumer/owner identifier recorded by the server. */
  const char *owner;
  /** Optional transaction identifier to bind the dequeue into. */
  const char *txn_id;
  /** Visibility timeout granted when a message is delivered. */
  long visibility_timeout_seconds;
  /** Long-poll wait in seconds before returning no message. */
  long wait_seconds;
  /** Maximum number of messages requested in one server page. */
  int page_size;
  /** Queue cursor used to continue from a previous position. */
  const char *start_after;
} lc_dequeue_req;

/** Request used to fetch queue-level statistics. */
typedef struct lc_queue_stats_req {
  const char *namespace_name;
  const char *queue;
} lc_queue_stats_req;

/** Queue-level metrics and head-of-line metadata. */
typedef struct lc_queue_stats_res {
  char *namespace_name;
  char *queue;
  int waiting_consumers;
  int pending_candidates;
  int total_consumers;
  int has_active_watcher;
  int available;
  char *head_message_id;
  long head_enqueued_at_unix;
  long head_not_visible_until_unix;
  long head_age_seconds;
  char *correlation_id;
} lc_queue_stats_res;

/** Batch result returned by `dequeue_batch()`. */
typedef struct lc_dequeue_batch_res {
  /** Delivered messages in dequeue order. */
  lc_message **messages;
  /** Number of entries in `messages`. */
  size_t count;
} lc_dequeue_batch_res;

/** Stable queue message identity used by client-level queue operations. */
typedef struct lc_message_ref {
  /** Namespace of the message. */
  const char *namespace_name;
  /** Queue containing the message. */
  const char *queue;
  /** Server-issued message identifier. */
  const char *message_id;
  /** Delivery lease identifier for the queue message. */
  const char *lease_id;
  /** Transaction identifier associated with the message, if any. */
  const char *txn_id;
  /** Current fencing token for the message delivery lease. */
  long fencing_token;
  /** Current queue metadata etag. */
  const char *meta_etag;
  /** Current state etag when the message carries state. */
  const char *state_etag;
  /** Associated state lease identifier when the message carries state. */
  const char *state_lease_id;
  /** Fencing token for the associated state lease. */
  long state_fencing_token;
} lc_message_ref;

/** Result returned by queue ack operations. */
typedef struct lc_ack_res {
  int acked;
  char *correlation_id;
} lc_ack_res;

/** Client-level ack operation on an existing queue message reference. */
typedef struct lc_ack_op {
  lc_message_ref message;
} lc_ack_op;

/**
 * Public nack intent for queue redelivery semantics.
 *
 * This is intentionally an enum in the SDK surface so callers cannot send
 * arbitrary transport strings by mistake. The transport still serializes the
 * server contract values (`failure` or `defer`) internally.
 */
typedef enum lc_nack_intent {
  /**
   * Default nack behavior.
   *
   * This maps to the server's `failure` intent and counts as a processing
   * failure for queue retry budgeting.
   */
  LC_NACK_INTENT_UNSPECIFIED = 0,
  /**
   * Processing failure.
   *
   * Use this when the handler attempted to process the message and failed.
   * This maps to the server's `failure` transport value and consumes the
   * message's failure budget.
   */
  LC_NACK_INTENT_FAILURE = 1,
  /**
   * Intentional deferral.
   *
   * Use this when processing should be retried later without counting as a
   * handler failure. This maps to the server's `defer` transport value and
   * does not consume the message's failure budget.
   */
  LC_NACK_INTENT_DEFER = 2
} lc_nack_intent;

/** Request used to negatively acknowledge and optionally requeue a message. */
typedef struct lc_nack_req {
  /** Delay in seconds before the message becomes visible again. */
  long delay_seconds;
  /**
   * Redelivery intent for the nack.
   *
   * `LC_NACK_INTENT_UNSPECIFIED` defaults to `LC_NACK_INTENT_FAILURE`.
   * Use `LC_NACK_INTENT_DEFER` for deliberate retry-later behavior that should
   * not count against failure-based retry limits.
   */
  lc_nack_intent intent;
  /**
   * Optional JSON object describing the last processing error.
   *
   * This is meaningful for `LC_NACK_INTENT_FAILURE`. Leave it `NULL` for
   * `LC_NACK_INTENT_DEFER` unless you intentionally want to record failure
   * detail alongside the nack.
   */
  const char *last_error_json;
} lc_nack_req;

/** Client-level nack operation on an existing queue message reference. */
typedef struct lc_nack_op {
  lc_message_ref message;
  /** Delay in seconds before the message becomes visible again. */
  long delay_seconds;
  /**
   * Redelivery intent for the nack.
   *
   * `LC_NACK_INTENT_UNSPECIFIED` defaults to `LC_NACK_INTENT_FAILURE`.
   * Use `LC_NACK_INTENT_DEFER` to requeue intentionally without consuming the
   * message's failure budget.
   */
  lc_nack_intent intent;
  /**
   * Optional JSON object describing the last processing error.
   *
   * This is typically set for `LC_NACK_INTENT_FAILURE` and omitted for
   * `LC_NACK_INTENT_DEFER`.
   */
  const char *last_error_json;
} lc_nack_op;

/** Result returned by queue nack operations. */
typedef struct lc_nack_res {
  int requeued;
  char *meta_etag;
  char *correlation_id;
} lc_nack_res;

/** Request used to extend queue message visibility. */
typedef struct lc_extend_req {
  /** Additional visibility time in seconds. */
  long extend_by_seconds;
} lc_extend_req;

/** Client-level extend operation on an existing queue message reference. */
typedef struct lc_extend_op {
  lc_message_ref message;
  long extend_by_seconds;
} lc_extend_op;

/** Result returned by queue visibility extension operations. */
typedef struct lc_extend_res {
  long lease_expires_at_unix;
  long visibility_timeout_seconds;
  char *meta_etag;
  long state_lease_expires_at_unix;
  char *correlation_id;
} lc_extend_res;

/** Request used to watch queue availability changes. */
typedef struct lc_watch_queue_req {
  const char *namespace_name;
  const char *queue;
} lc_watch_queue_req;

/** Event delivered to queue watch handlers. */
typedef struct lc_watch_event {
  char *namespace_name;
  char *queue;
  int available;
  char *head_message_id;
  long changed_at_unix;
  char *correlation_id;
} lc_watch_event;

/** Callback invoked for each queue watch event. */
typedef int (*lc_watch_handler_fn)(void *context, const lc_watch_event *event,
                                   lc_error *error);

/** Queue watch callback registration. */
typedef struct lc_watch_handler {
  lc_watch_handler_fn handle;
  void *context;
} lc_watch_handler;

/** Queue consumer callback registration used by subscribe flows. */
typedef struct lc_consumer {
  int (*handle)(void *context, lc_message *message, lc_error *error);
  void *context;
} lc_consumer;

/**
 * Restart policy for one managed queue consumer loop.
 *
 * Defaults match the Go SDK `StartConsumer` behavior: three immediate retries,
 * then exponential backoff starting at 250ms with multiplier 2.0 and a
 * 5-minute cap.
 */
typedef struct lc_consumer_restart_policy {
  /** Number of consecutive failures retried without delay. */
  int immediate_retries;
  /** Initial delayed retry interval in milliseconds. */
  long base_delay_ms;
  /** Maximum retry delay in milliseconds. */
  long max_delay_ms;
  /** Exponential growth multiplier applied after immediate retries. */
  double multiplier;
  /** Jitter applied as +/- `jitter_ms` around the computed delay. */
  long jitter_ms;
  /** Maximum consecutive failures before the consumer stops. Zero retries
   * forever. */
  int max_failures;
} lc_consumer_restart_policy;

/**
 * Delivery object passed to managed consumer callbacks.
 *
 * All pointers are borrowed for the duration of the callback only. The handler
 * should terminate the delivery with `message->ack()` or `message->nack()`.
 * When `state` is non-NULL, it is the lease handle associated with the
 * delivery and is owned by `message`.
 */
typedef struct lc_consumer_message {
  /** Active SDK client for this consumer loop. Safe to reuse inside the
   * callback. */
  lc_client *client;
  /** Borrowed client logger configured on the parent client. Always non-NULL.
   */
  pslog_logger *logger;
  /** Resolved consumer name. Defaults to the queue name when omitted. */
  const char *name;
  /** Queue currently being consumed. */
  const char *queue;
  /** Non-zero when this delivery includes an attached state lease. */
  int with_state;
  /** Delivered queue message handle. */
  lc_message *message;
  /** Attached state lease, or `NULL` for stateless consumers. */
  lc_lease *state;
} lc_consumer_message;

/**
 * Recoverable consumer-loop failure reported before restart.
 *
 * The `cause` object is borrowed for the duration of the callback only.
 */
typedef struct lc_consumer_error {
  /** Resolved consumer name. */
  const char *name;
  /** Queue whose loop failed. */
  const char *queue;
  /** Non-zero when the failing loop used `dequeue_with_state()`. */
  int with_state;
  /** Current consecutive failure count for this consumer. */
  int attempt;
  /** Delay before the next retry, in milliseconds. Zero means immediate retry.
   */
  long restart_in_ms;
  /** Error that caused the failure. */
  const lc_error *cause;
} lc_consumer_error;

/**
 * Lifecycle transition reported for one managed consumer loop attempt.
 *
 * The optional `error` pointer is borrowed for the duration of the callback.
 */
typedef struct lc_consumer_lifecycle_event {
  /** Resolved consumer name. */
  const char *name;
  /** Queue whose loop transitioned. */
  const char *queue;
  /** Non-zero when the loop uses `dequeue_with_state()`. */
  int with_state;
  /** One-based attempt sequence number for this consumer loop. */
  int attempt;
  /** Terminal error for the attempt, or `NULL` on clean start/stop. */
  const lc_error *error;
} lc_consumer_lifecycle_event;

/**
 * One managed consumer loop configuration used by `lc_consumer_service`.
 *
 * The service will default `request.owner` when it is empty, default
 * `request.namespace_name` from the client when it is empty, and then run a
 * long-lived streaming subscribe loop that calls `handle()` once per
 * delivery.
 */
typedef struct lc_consumer_config {
  /** Logical consumer name used in events and owner defaulting. */
  const char *name;
  /** Dequeue options for this consumer. `request.queue` is required. */
  lc_dequeue_req request;
  /**
   * Number of parallel subscribe workers for this consumer config.
   *
   * Each worker runs its own subscribe loop and invokes `handle()`
   * sequentially for the deliveries it receives. Zero defaults to `1`.
   */
  size_t worker_count;
  /** Non-zero to use `dequeue_with_state()` and populate
   * `lc_consumer_message.state`. */
  int with_state;
  /**
   * Handles one delivery.
   *
   * Return `LC_OK` after successfully terminating the message with `ack()` or
   * `nack()`. Returning a non-`LC_OK` code marks the attempt as failed and
   * enters the restart/error path.
   */
  int (*handle)(void *context, lc_consumer_message *message, lc_error *error);
  /**
   * Observes a failed loop before restart.
   *
   * Return `LC_OK` to continue restarting. Returning a non-`LC_OK` code stops
   * the service and returns that error from `run()`/`wait()`.
   */
  int (*on_error)(void *context, const lc_consumer_error *event,
                  lc_error *error);
  /** Called when one consumer attempt starts. */
  void (*on_start)(void *context, const lc_consumer_lifecycle_event *event);
  /** Called when one consumer attempt stops. */
  void (*on_stop)(void *context, const lc_consumer_lifecycle_event *event);
  /** Opaque user context passed to all callbacks for this consumer. */
  void *context;
  /** Restart behavior after handler or transport failures. */
  lc_consumer_restart_policy restart_policy;
} lc_consumer_config;

/**
 * Settings used to create a managed queue consumer service.
 *
 * The service owns a deep copy of these configs after successful creation.
 */
typedef struct lc_consumer_service_config {
  /** Array of consumer loop definitions. */
  const lc_consumer_config *consumers;
  /** Number of entries in `consumers`. */
  size_t consumer_count;
} lc_consumer_service_config;

/** Attachment selector by id or name. */
typedef struct lc_attachment_selector {
  const char *id;
  const char *name;
} lc_attachment_selector;

/** Attachment metadata returned by list, fetch, and attach operations. */
typedef struct lc_attachment_info {
  char *id;
  char *name;
  long size;
  char *plaintext_sha256;
  char *content_type;
  long created_at_unix;
  long updated_at_unix;
} lc_attachment_info;

/** Request used to attach streamed content to a lease. */
typedef struct lc_attach_req {
  /** Attachment name recorded by the server. */
  const char *name;
  /** Content type recorded with the attachment. */
  const char *content_type;
  /** Maximum accepted upload size when `has_max_bytes` is non-zero. */
  long max_bytes;
  /** Enables enforcement of `max_bytes`. */
  int has_max_bytes;
  /** Rejects overwriting an existing attachment with the same name. */
  int prevent_overwrite;
} lc_attach_req;

/** Client-level attachment upload operation for an existing lease reference. */
typedef struct lc_attach_op {
  lc_lease_ref lease;
  const char *name;
  const char *content_type;
  long max_bytes;
  int has_max_bytes;
  int prevent_overwrite;
} lc_attach_op;

/** Result returned by an attachment upload. */
typedef struct lc_attach_res {
  lc_attachment_info attachment;
  int noop;
  long version;
  char *correlation_id;
} lc_attach_res;

/** Attachment listing returned by list operations. */
typedef struct lc_attachment_list {
  lc_attachment_info *items;
  size_t count;
  char *correlation_id;
} lc_attachment_list;

/** Request used to fetch an attachment stream. */
typedef struct lc_attachment_get_req {
  lc_attachment_selector selector;
  int public_read;
} lc_attachment_get_req;

/** Client-level request used to list lease attachments. */
typedef struct lc_attachment_list_req {
  lc_lease_ref lease;
  int public_read;
} lc_attachment_list_req;

/** Client-level attachment fetch operation for an existing lease reference. */
typedef struct lc_attachment_get_op {
  lc_lease_ref lease;
  lc_attachment_selector selector;
  int public_read;
} lc_attachment_get_op;

/** Client-level attachment delete operation for an existing lease reference. */
typedef struct lc_attachment_delete_op {
  lc_lease_ref lease;
  lc_attachment_selector selector;
} lc_attachment_delete_op;

/** Client-level operation used to delete all attachments for a lease reference.
 */
typedef struct lc_attachment_delete_all_op {
  lc_lease_ref lease;
} lc_attachment_delete_all_op;

/** Result metadata returned by attachment fetch operations. */
typedef struct lc_attachment_get_res {
  lc_attachment_info attachment;
  char *correlation_id;
} lc_attachment_get_res;

/**
 * Lease handle with direct methods and read-only identity metadata.
 *
 * This is the normal handle returned from `acquire()` and from queue flows that
 * carry state. Use it for the common lease lifecycle:
 *
 * `describe()` or read the published fields, `get()`/`load()` the state,
 * `update()`/`save()`/`mutate()` it, `keepalive()` while the lease is held, and
 * finally `release()` to release the server-side lease and close the handle.
 *
 * If you want to abandon the local handle without a server-side release, call
 * `close()`.
 */
struct lc_lease {
  /** Refreshes the published lease fields on `self` from the server. */
  int (*describe)(lc_lease *self, lc_error *error);
  /**
   * Streams the current state document into `dst`.
   *
   * Use this for large documents. The caller owns `dst` and closes it.
   * `opts` may be `NULL` for the default read behavior.
   */
  int (*get)(lc_lease *self, lc_sink *dst, const lc_get_opts *opts,
             lc_get_res *out, lc_error *error);
  /**
   * Convenience variant of `get()` that materializes the state document into a
   * newly allocated JSON text buffer.
   *
   * The caller owns `*json_text` on success and frees it with the client
   * allocator or the default allocator.
   */
  int (*load)(lc_lease *self, char **json_text, size_t *json_length,
              const lc_get_opts *opts, lc_get_res *out, lc_error *error);
  /**
   * Convenience variant of `update()` for small in-memory JSON documents.
   *
   * For large documents, prefer `update()` with an `lc_json`.
   */
  int (*save)(lc_lease *self, const char *json_text, lc_error *error);
  /**
   * Replaces the state document from a streamed JSON source.
   *
   * Use this for large JSON payloads. `opts` may be `NULL` for default update
   * behavior. On success, `self->version` and `self->state_etag` are refreshed.
   */
  int (*update)(lc_lease *self, lc_json *json, const lc_update_opts *opts,
                lc_error *error);
  /**
   * Applies one or more server-side mutations to the current state.
   *
   * On success, the published version and etag fields on `self` are refreshed.
   */
  int (*mutate)(lc_lease *self, const lc_mutate_req *req, lc_error *error);
  /**
   * Streams the current state through the local mutate engine and uploads the
   * staged JSON result back to lockd.
   *
   * This is the bounded-memory path for `file:`, `textfile:`, and
   * `base64file:` mutators. On success, `self->version` and
   * `self->state_etag` are refreshed from the update response.
   */
  int (*mutate_local)(lc_lease *self, const lc_mutate_local_req *req,
                      lc_error *error);
  /**
   * Updates lease metadata without replacing the state bytes.
   *
   * On success, the published query-hidden fields on `self` are refreshed.
   */
  int (*metadata)(lc_lease *self, const lc_metadata_req *req, lc_error *error);
  /**
   * Removes the current state bytes while keeping the lease itself alive.
   *
   * Use this when the lease should continue to exist but the state document
   * should not.
   */
  int (*remove)(lc_lease *self, const lc_remove_req *req, lc_error *error);
  /**
   * Renews the lease TTL and refreshes the expiry-related fields on `self`.
   *
   * Call this while the lease is still in use.
   */
  int (*keepalive)(lc_lease *self, const lc_keepalive_req *req,
                   lc_error *error);
  /**
   * Releases the lease on the server and closes the handle on success.
   *
   * This is the normal terminal step for an acquired lease. If it fails, the
   * handle remains valid and may be retried or closed locally with `close()`.
   */
  int (*release)(lc_lease *self, const lc_release_req *req, lc_error *error);
  /**
   * Streams an attachment upload from `src`.
   *
   * Use this for large binary payloads that must not be buffered in memory.
   */
  int (*attach)(lc_lease *self, const lc_attach_req *req, lc_source *src,
                lc_attach_res *out, lc_error *error);
  /** Lists the attachments currently associated with the lease. */
  int (*list_attachments)(lc_lease *self, lc_attachment_list *out,
                          lc_error *error);
  /**
   * Streams an attachment download into `dst`.
   *
   * Use this for large attachments. The caller owns `dst` and closes it.
   */
  int (*get_attachment)(lc_lease *self, const lc_attachment_get_req *req,
                        lc_sink *dst, lc_attachment_get_res *out,
                        lc_error *error);
  /** Deletes a single attachment selected by name or digest. */
  int (*delete_attachment)(lc_lease *self,
                           const lc_attachment_selector *selector, int *deleted,
                           lc_error *error);
  /** Deletes every attachment currently associated with the lease. */
  int (*delete_all_attachments)(lc_lease *self, int *deleted_count,
                                lc_error *error);
  /**
   * Closes the local handle without issuing a server-side release.
   *
   * Use this only when abandoning the handle or after a failed `release()`.
   */
  void (*close)(lc_lease *self);

  /** Namespace of the bound lease. */
  const char *namespace_name;
  /** Key of the bound lease. */
  const char *key;
  /** Current recorded owner of the lease. */
  const char *owner;
  /** Server-issued lease identifier. */
  const char *lease_id;
  /** Transaction identifier associated with the lease, if any. */
  const char *txn_id;
  /** Current fencing token for optimistic concurrency. */
  long fencing_token;
  /** Current state version published by the server. */
  long version;
  /** Current lease expiry as a Unix timestamp. */
  long lease_expires_at_unix;
  /** Current private state etag. */
  const char *state_etag;
  /** Non-zero when `query_hidden` was explicitly set by the server. */
  int has_query_hidden;
  /** Whether the state is hidden from normal query results. */
  int query_hidden;

  void *impl;
};

/**
 * Queue message handle with direct lifecycle and payload access methods.
 *
 * This is the normal handle returned from `dequeue()` and subscription flows.
 * Read or stream the payload, optionally inspect `state()`, then terminate the
 * delivery with `ack()` or `nack()`. If you need to abandon the local handle
 * without acknowledging it, call `close()`.
 */
struct lc_message {
  /**
   * Acknowledges the message and closes the handle on success.
   *
   * If this call fails, the handle remains valid and may be retried or closed.
   */
  int (*ack)(lc_message *self, lc_error *error);
  /**
   * Negatively acknowledges the message and closes the handle on success.
   *
   * `req->intent` selects whether the nack is treated as a processing
   * `failure` or an intentional `defer`. If this call fails, the handle
   * remains valid and may be retried or closed.
   */
  int (*nack)(lc_message *self, const lc_nack_req *req, lc_error *error);
  /**
   * Extends message visibility and refreshes the expiry fields on `self`.
   *
   * This is non-terminal: the message remains open after success.
   */
  int (*extend)(lc_message *self, const lc_extend_req *req, lc_error *error);
  /**
   * Returns the associated lease when the dequeue variant includes state.
   *
   * The returned handle is owned by the message and is closed with the message.
   */
  lc_lease *(*state)(lc_message *self);
  /**
   * Returns the payload reader owned by the message handle.
   *
   * Do not close the returned source directly; close the message instead. Some
   * live subscribe deliveries are single-pass streams and may not support
   * `reset()`.
   */
  lc_source *(*payload_reader)(lc_message *self);
  /** Rewinds the payload stream when the underlying source supports it. */
  int (*rewind_payload)(lc_message *self, lc_error *error);
  /** Copies the payload stream into `dst`. */
  int (*write_payload)(lc_message *self, lc_sink *dst, size_t *written,
                       lc_error *error);
  /**
   * Closes the local message handle without acknowledging it.
   *
   * Use this only when abandoning the handle or after a failed terminal call.
   */
  void (*close)(lc_message *self);

  /** Namespace containing the queue message. */
  const char *namespace_name;
  /** Queue name. */
  const char *queue;
  /** Server-issued message identifier. */
  const char *message_id;
  /** Number of delivery attempts so far. */
  int attempts;
  /** Maximum delivery attempts configured for the message. */
  int max_attempts;
  /** Number of failed delivery attempts recorded by the server. */
  int failure_attempts;
  /** Unix timestamp until which the message is hidden from other consumers. */
  long not_visible_until_unix;
  /** Current visibility timeout in seconds. */
  long visibility_timeout_seconds;
  /** Payload content type recorded with the message. */
  const char *payload_content_type;
  /** Server correlation identifier for the dequeue response. */
  const char *correlation_id;
  /** Delivery lease identifier for the message. */
  const char *lease_id;
  /** Current delivery lease expiry as a Unix timestamp. */
  long lease_expires_at_unix;
  /** Current fencing token for the delivery lease. */
  long fencing_token;
  /** Transaction identifier associated with the message, if any. */
  const char *txn_id;
  /** Current queue metadata etag. */
  const char *meta_etag;
  /** Cursor for the next queue page when the server returned one. */
  const char *next_cursor;
  /** Internal payload reader handle owned by the message. */
  lc_source *payload;

  void *impl;
};

/**
 * Managed queue consumer service.
 *
 * Create this from `client->new_consumer_service()`, then call `run()` for the
 * blocking Go-`StartConsumer` style workflow or `start()`/`stop()`/`wait()` for
 * explicit daemon-style integration.
 */
struct lc_consumer_service {
  /**
   * Starts all configured consumer loops and blocks until they stop.
   *
   * This is the closest equivalent to the Go SDK `StartConsumer` call.
   * Returning `LC_OK` means the service stopped cleanly, either because
   * `stop()` was requested or because all loops exited without a fatal error.
   */
  int (*run)(lc_consumer_service *self, lc_error *error);
  /**
   * Starts all configured consumer loops asynchronously.
   *
   * Pair this with `stop()` and `wait()` when embedding the service into a
   * larger daemon that owns its own main loop.
   */
  int (*start)(lc_consumer_service *self, lc_error *error);
  /**
   * Requests shutdown of all consumer loops.
   *
   * This does not free the service object. Call `wait()` to join all worker
   * threads, then `close()` when the service is no longer needed.
   */
  int (*stop)(lc_consumer_service *self);
  /**
   * Blocks until all consumer loops exit and returns the terminal service
   * result.
   *
   * `LC_OK` means clean shutdown. Any non-`LC_OK` result is the first fatal
   * service error captured during execution.
   */
  int (*wait)(lc_consumer_service *self, lc_error *error);
  /**
   * Closes the local service object and releases all associated resources.
   *
   * If the service is still running, `close()` first requests shutdown and
   * waits for all loops to exit.
   */
  void (*close)(lc_consumer_service *self);

  void *impl;
};

/**
 * Root client handle.
 *
 * This is the root object for the SDK. Open it once with `lc_client_open()`,
 * then use it to acquire leases, query state, enqueue or dequeue messages, and
 * call the admin surfaces.
 *
 * For the common path, `acquire()` returns an `lc_lease` handle and
 * `dequeue()` returns an `lc_message` handle. Those returned handles own their
 * ongoing lifecycle and should normally be finished with `release()`/`ack()`
 * rather than jumping back to client-level calls.
 */
struct lc_client {
  /**
   * Acquires a new lease and returns an `lc_lease` handle on success.
   *
   * The returned handle is the normal object for follow-up calls such as
   * `lease->update()`, `lease->get()`, `lease->keepalive()`, and
   * `lease->release()`.
   */
  int (*acquire)(lc_client *self, const lc_acquire_req *req, lc_lease **out,
                 lc_error *error);
  /** Describes the current state and lease metadata for `req->key`. */
  int (*describe)(lc_client *self, const lc_describe_req *req,
                  lc_describe_res *out, lc_error *error);
  /** Streams the current state document for `key` into `dst`. */
  int (*get)(lc_client *self, const char *key, const lc_get_opts *opts,
             lc_sink *dst, lc_get_res *out, lc_error *error);
  /** Convenience variant of `get()` that materializes the state into memory. */
  int (*load)(lc_client *self, const char *key, const lc_get_opts *opts,
              char **json_text, size_t *json_length, lc_get_res *out,
              lc_error *error);
  /** Updates an existing lease reference from a streamed JSON source. */
  int (*update)(lc_client *self, const lc_update_req *req, lc_json *json,
                lc_update_res *out, lc_error *error);
  /** Applies one or more server-side mutations to an existing lease reference.
   */
  int (*mutate)(lc_client *self, const lc_mutate_op *req, lc_mutate_res *out,
                lc_error *error);
  /** Updates metadata fields on an existing lease reference. */
  int (*metadata)(lc_client *self, const lc_metadata_op *req,
                  lc_metadata_res *out, lc_error *error);
  /** Removes the state bytes for an existing lease reference. */
  int (*remove)(lc_client *self, const lc_remove_op *req, lc_remove_res *out,
                lc_error *error);
  /** Renews an existing lease reference without using a bound `lc_lease`. */
  int (*keepalive)(lc_client *self, const lc_keepalive_op *req,
                   lc_keepalive_res *out, lc_error *error);
  /** Releases an existing lease reference without using a bound `lc_lease`. */
  int (*release)(lc_client *self, const lc_release_op *req, lc_release_res *out,
                 lc_error *error);
  /** Streams an attachment upload for an existing lease reference. */
  int (*attach)(lc_client *self, const lc_attach_op *req, lc_source *src,
                lc_attach_res *out, lc_error *error);
  /** Lists the attachments associated with an existing lease reference. */
  int (*list_attachments)(lc_client *self, const lc_attachment_list_req *req,
                          lc_attachment_list *out, lc_error *error);
  /** Streams an attachment download for an existing lease reference. */
  int (*get_attachment)(lc_client *self, const lc_attachment_get_op *req,
                        lc_sink *dst, lc_attachment_get_res *out,
                        lc_error *error);
  /** Deletes one attachment associated with an existing lease reference. */
  int (*delete_attachment)(lc_client *self, const lc_attachment_delete_op *req,
                           int *deleted, lc_error *error);
  /** Deletes all attachments associated with an existing lease reference. */
  int (*delete_all_attachments)(lc_client *self,
                                const lc_attachment_delete_all_op *req,
                                int *deleted_count, lc_error *error);
  /** Returns queue depth and delivery metadata for a queue. */
  int (*queue_stats)(lc_client *self, const lc_queue_stats_req *req,
                     lc_queue_stats_res *out, lc_error *error);
  /** Acknowledges a queue message from client-level identifiers. */
  int (*queue_ack)(lc_client *self, const lc_ack_op *req, lc_ack_res *out,
                   lc_error *error);
  /** Negatively acknowledges a queue message from client-level identifiers. */
  int (*queue_nack)(lc_client *self, const lc_nack_op *req, lc_nack_res *out,
                    lc_error *error);
  /** Extends visibility for a queue message from client-level identifiers. */
  int (*queue_extend)(lc_client *self, const lc_extend_op *req,
                      lc_extend_res *out, lc_error *error);
  /** Streams query results into `dst`. */
  int (*query)(lc_client *self, const lc_query_req *req, lc_sink *dst,
               lc_query_res *out, lc_error *error);
  /** Reads namespace-level engine configuration. */
  int (*get_namespace_config)(lc_client *self,
                              const lc_namespace_config_req *req,
                              lc_namespace_config_res *out, lc_error *error);
  /** Updates namespace-level engine configuration. */
  int (*update_namespace_config)(lc_client *self,
                                 const lc_namespace_config_req *req,
                                 lc_namespace_config_res *out, lc_error *error);
  /** Triggers an index flush for the selected namespace. */
  int (*flush_index)(lc_client *self, const lc_index_flush_req *req,
                     lc_index_flush_res *out, lc_error *error);
  /** Replays a transaction by transaction identifier. */
  int (*txn_replay)(lc_client *self, const lc_txn_replay_req *req,
                    lc_txn_replay_res *out, lc_error *error);
  /** Prepares a transaction decision. */
  int (*txn_prepare)(lc_client *self, const lc_txn_decision_req *req,
                     lc_txn_decision_res *out, lc_error *error);
  /** Commits a transaction decision. */
  int (*txn_commit)(lc_client *self, const lc_txn_decision_req *req,
                    lc_txn_decision_res *out, lc_error *error);
  /** Rolls back a transaction decision. */
  int (*txn_rollback)(lc_client *self, const lc_txn_decision_req *req,
                      lc_txn_decision_res *out, lc_error *error);
  /** Acquires a TC lease. */
  int (*tc_lease_acquire)(lc_client *self, const lc_tc_lease_acquire_req *req,
                          lc_tc_lease_acquire_res *out, lc_error *error);
  /** Renews a TC lease. */
  int (*tc_lease_renew)(lc_client *self, const lc_tc_lease_renew_req *req,
                        lc_tc_lease_renew_res *out, lc_error *error);
  /** Releases a TC lease. */
  int (*tc_lease_release)(lc_client *self, const lc_tc_lease_release_req *req,
                          lc_tc_lease_release_res *out, lc_error *error);
  /** Returns current TC leader information. */
  int (*tc_leader)(lc_client *self, lc_tc_leader_res *out, lc_error *error);
  /** Announces the current node into the TC cluster set. */
  int (*tc_cluster_announce)(lc_client *self,
                             const lc_tc_cluster_announce_req *req,
                             lc_tc_cluster_res *out, lc_error *error);
  /** Removes the current node from the TC cluster set. */
  int (*tc_cluster_leave)(lc_client *self, lc_tc_cluster_res *out,
                          lc_error *error);
  /** Lists the current TC cluster set. */
  int (*tc_cluster_list)(lc_client *self, lc_tc_cluster_res *out,
                         lc_error *error);
  /** Registers a TC resource manager. */
  int (*tc_rm_register)(lc_client *self, const lc_tc_rm_register_req *req,
                        lc_tc_rm_res *out, lc_error *error);
  /** Unregisters a TC resource manager. */
  int (*tc_rm_unregister)(lc_client *self, const lc_tc_rm_unregister_req *req,
                          lc_tc_rm_res *out, lc_error *error);
  /** Lists registered TC resource managers. */
  int (*tc_rm_list)(lc_client *self, lc_tc_rm_list_res *out, lc_error *error);
  /** Streams a queue payload upload and enqueues it. */
  int (*enqueue)(lc_client *self, const lc_enqueue_req *req, lc_source *src,
                 lc_enqueue_res *out, lc_error *error);
  /** Dequeues a queue message without an associated state lease. */
  int (*dequeue)(lc_client *self, const lc_dequeue_req *req, lc_message **out,
                 lc_error *error);
  /** Dequeues up to `req->page_size` messages and returns them as a batch. */
  int (*dequeue_batch)(lc_client *self, const lc_dequeue_req *req,
                       lc_dequeue_batch_res *out, lc_error *error);
  /** Dequeues a queue message and includes an associated state lease handle. */
  int (*dequeue_with_state)(lc_client *self, const lc_dequeue_req *req,
                            lc_message **out, lc_error *error);
  /** Consumes queue messages with a streaming callback. */
  int (*subscribe)(lc_client *self, const lc_dequeue_req *req,
                   const lc_consumer *consumer, lc_error *error);
  /** Consumes queue messages with attached state leases. */
  int (*subscribe_with_state)(lc_client *self, const lc_dequeue_req *req,
                              const lc_consumer *consumer, lc_error *error);
  /**
   * Creates a managed consumer service that mirrors the Go SDK
   * `StartConsumer` workflow.
   *
   * The returned service owns deep copies of the consumer configs and may
   * outlive the root client. Each worker uses its own cloned client instance so
   * retries and shutdown do not invalidate the caller's root handle.
   */
  int (*new_consumer_service)(lc_client *self,
                              const lc_consumer_service_config *config,
                              lc_consumer_service **out, lc_error *error);
  /** Watches queue depth changes with a streaming watch callback. */
  int (*watch_queue)(lc_client *self, const lc_watch_queue_req *req,
                     const lc_watch_handler *handler, lc_error *error);
  /**
   * Closes the client and invalidates any derived lease or message handles.
   *
   * Call this once when all work with the client is finished.
   */
  void (*close)(lc_client *self);

  /** Default namespace applied when request structs leave `namespace_name`
   * unset. */
  const char *default_namespace;
  void *impl;
};

/** Returns the semantic version string compiled into this build. */
const char *lc_version_string(void);

/** Resets an `lc_error` to a known empty state. */
void lc_error_init(lc_error *error);
/** Releases all heap-owned fields inside an `lc_error`. */
void lc_error_cleanup(lc_error *error);
/** Initializes an allocator override to use the default allocator. */
void lc_allocator_init(lc_allocator *allocator);
/** Initializes client configuration with safe defaults. */
void lc_client_config_init(lc_client_config *config);
/** Initializes a lease reference to all-zero/empty values. */
void lc_lease_ref_init(lc_lease_ref *lease);
/** Initializes an acquire request to all-zero/empty values. */
void lc_acquire_req_init(lc_acquire_req *request);
/** Initializes a describe request to all-zero/empty values. */
void lc_describe_req_init(lc_describe_req *request);
/** Initializes get options to all-zero/empty values. */
void lc_get_opts_init(lc_get_opts *options);
/** Initializes update options to all-zero/empty values. */
void lc_update_opts_init(lc_update_opts *options);
/** Initializes a client-level update request to all-zero/empty values. */
void lc_update_req_init(lc_update_req *request);
/** Initializes a mutate request to all-zero/empty values. */
void lc_mutate_req_init(lc_mutate_req *request);
/** Initializes a local mutate request to all-zero/empty values. */
void lc_mutate_local_req_init(lc_mutate_local_req *request);
/** Initializes a client-level mutate operation to all-zero/empty values. */
void lc_mutate_op_init(lc_mutate_op *request);
/** Initializes a metadata request to all-zero/empty values. */
void lc_metadata_req_init(lc_metadata_req *request);
/** Initializes a client-level metadata operation to all-zero/empty values. */
void lc_metadata_op_init(lc_metadata_op *request);
/** Initializes a remove request to all-zero/empty values. */
void lc_remove_req_init(lc_remove_req *request);
/** Initializes a client-level remove operation to all-zero/empty values. */
void lc_remove_op_init(lc_remove_op *request);
/** Initializes a keepalive request to all-zero/empty values. */
void lc_keepalive_req_init(lc_keepalive_req *request);
/** Initializes a client-level keepalive operation to all-zero/empty values. */
void lc_keepalive_op_init(lc_keepalive_op *request);
/** Initializes a release request to all-zero/empty values. */
void lc_release_req_init(lc_release_req *request);
/** Initializes a client-level release operation to all-zero/empty values. */
void lc_release_op_init(lc_release_op *request);
/** Initializes a query request to all-zero/empty values. */
void lc_query_req_init(lc_query_req *request);
/** Initializes a namespace configuration request to all-zero/empty values. */
void lc_namespace_config_req_init(lc_namespace_config_req *request);
/** Initializes an index flush request to all-zero/empty values. */
void lc_index_flush_req_init(lc_index_flush_req *request);
/** Initializes a transaction replay request to all-zero/empty values. */
void lc_txn_replay_req_init(lc_txn_replay_req *request);
/** Initializes a transaction decision request to all-zero/empty values. */
void lc_txn_decision_req_init(lc_txn_decision_req *request);
/** Initializes a TC lease acquire request to all-zero/empty values. */
void lc_tc_lease_acquire_req_init(lc_tc_lease_acquire_req *request);
/** Initializes a TC lease renew request to all-zero/empty values. */
void lc_tc_lease_renew_req_init(lc_tc_lease_renew_req *request);
/** Initializes a TC lease release request to all-zero/empty values. */
void lc_tc_lease_release_req_init(lc_tc_lease_release_req *request);
/** Initializes a TC cluster announce request to all-zero/empty values. */
void lc_tc_cluster_announce_req_init(lc_tc_cluster_announce_req *request);
/** Initializes a TC RM register request to all-zero/empty values. */
void lc_tc_rm_register_req_init(lc_tc_rm_register_req *request);
/** Initializes a TC RM unregister request to all-zero/empty values. */
void lc_tc_rm_unregister_req_init(lc_tc_rm_unregister_req *request);
/** Initializes an enqueue request to all-zero/empty values. */
void lc_enqueue_req_init(lc_enqueue_req *request);
/** Initializes a dequeue request to all-zero/empty values. */
void lc_dequeue_req_init(lc_dequeue_req *request);
/** Initializes a queue stats request to all-zero/empty values. */
void lc_queue_stats_req_init(lc_queue_stats_req *request);
/** Initializes a message reference to all-zero/empty values. */
void lc_message_ref_init(lc_message_ref *message);
/** Initializes a nack request to all-zero/empty values. */
void lc_nack_req_init(lc_nack_req *request);
/** Initializes a client-level nack operation to all-zero/empty values. */
void lc_nack_op_init(lc_nack_op *request);
/** Initializes an extend request to all-zero/empty values. */
void lc_extend_req_init(lc_extend_req *request);
/** Initializes a client-level extend operation to all-zero/empty values. */
void lc_extend_op_init(lc_extend_op *request);
/** Initializes a queue watch request to all-zero/empty values. */
void lc_watch_queue_req_init(lc_watch_queue_req *request);
/** Initializes a watch handler registration to all-zero/empty values. */
void lc_watch_handler_init(lc_watch_handler *handler);
/** Initializes a consumer registration to all-zero/empty values. */
void lc_consumer_init(lc_consumer *consumer);
/** Initializes a consumer restart policy to Go-SDK-compatible defaults. */
void lc_consumer_restart_policy_init(lc_consumer_restart_policy *policy);
/** Initializes one managed consumer config to all-zero/empty values. */
void lc_consumer_config_init(lc_consumer_config *config);
/** Initializes a consumer service config to all-zero/empty values. */
void lc_consumer_service_config_init(lc_consumer_service_config *config);
/** Initializes an attachment selector to all-zero/empty values. */
void lc_attachment_selector_init(lc_attachment_selector *selector);
/** Initializes an attachment upload request to all-zero/empty values. */
void lc_attach_req_init(lc_attach_req *request);
/** Initializes a client-level attachment upload operation to all-zero/empty
 * values. */
void lc_attach_op_init(lc_attach_op *request);
/** Initializes an attachment get request to all-zero/empty values. */
void lc_attachment_get_req_init(lc_attachment_get_req *request);
/** Initializes an attachment list request to all-zero/empty values. */
void lc_attachment_list_req_init(lc_attachment_list_req *request);
/** Initializes a client-level attachment get operation to all-zero/empty
 * values. */
void lc_attachment_get_op_init(lc_attachment_get_op *request);
/** Initializes a client-level attachment delete operation to all-zero/empty
 * values. */
void lc_attachment_delete_op_init(lc_attachment_delete_op *request);
/** Initializes a client-level delete-all-attachments operation to
 * all-zero/empty values. */
void lc_attachment_delete_all_op_init(lc_attachment_delete_all_op *request);

/**
 * Opens a new client handle.
 *
 * @param config Client construction settings.
 * @param out Receives the newly allocated client on success.
 * @param error Optional structured error output.
 * @return `LC_OK` on success, otherwise an error status code.
 */
int lc_client_open(const lc_client_config *config, lc_client **out,
                   lc_error *error);

/** Creates a rewindable source backed by caller-provided memory. */
int lc_source_from_memory(const void *bytes, size_t length, lc_source **out,
                          lc_error *error);
/** Opens a rewindable source backed by a file path. */
int lc_source_from_file(const char *path, lc_source **out, lc_error *error);
/** Wraps a file descriptor as a rewindable source when possible. */
int lc_source_from_fd(int fd, lc_source **out, lc_error *error);
/** Creates a sink that writes bytes to a file path. */
int lc_sink_to_file(const char *path, lc_sink **out, lc_error *error);
/** Creates a sink that writes bytes to a file descriptor. */
int lc_sink_to_fd(int fd, lc_sink **out, lc_error *error);
/** Creates an in-memory sink owned by the library. */
int lc_sink_to_memory(lc_sink **out, lc_error *error);
/** Returns the bytes accumulated by an in-memory sink. */
int lc_sink_memory_bytes(lc_sink *sink, const void **bytes, size_t *length,
                         lc_error *error);
/** Copies all bytes from a source into a sink. */
int lc_copy(lc_source *src, lc_sink *dst, size_t *written, lc_error *error);

/** Creates a rewindable JSON stream from a UTF-8 JSON string. */
int lc_json_from_string(const char *json_text, lc_json **out, lc_error *error);
/** Creates a rewindable JSON stream from a file path. */
int lc_json_from_file(const char *path, lc_json **out, lc_error *error);
/** Creates a rewindable JSON stream from a file descriptor. */
int lc_json_from_fd(int fd, lc_json **out, lc_error *error);
/** Adapts a generic source into a JSON stream and takes ownership of the source
 * handle. */
int lc_json_from_source(lc_source *source, lc_json **out, lc_error *error);
/** Closes and frees a client handle. */
void lc_client_close(lc_client *client);
/** Closes and frees a lease handle. */
void lc_lease_close(lc_lease *lease);
/** Closes and frees a message handle. */
void lc_message_close(lc_message *message);
/** Closes and frees a source. */
void lc_source_close(lc_source *source);
/** Closes and frees a sink. */
void lc_sink_close(lc_sink *sink);
/** Closes and frees a JSON source. */
void lc_json_close(lc_json *json);

/** Cleanup helpers for responses and metadata structs that own heap memory. */
void lc_describe_res_cleanup(lc_describe_res *response);
void lc_get_res_cleanup(lc_get_res *response);
void lc_update_res_cleanup(lc_update_res *response);
void lc_mutate_res_cleanup(lc_mutate_res *response);
void lc_metadata_res_cleanup(lc_metadata_res *response);
void lc_remove_res_cleanup(lc_remove_res *response);
void lc_keepalive_res_cleanup(lc_keepalive_res *response);
void lc_release_res_cleanup(lc_release_res *response);
void lc_query_res_cleanup(lc_query_res *response);
void lc_string_list_cleanup(lc_string_list *response);
void lc_namespace_config_res_cleanup(lc_namespace_config_res *response);
void lc_index_flush_res_cleanup(lc_index_flush_res *response);
void lc_txn_replay_res_cleanup(lc_txn_replay_res *response);
void lc_txn_decision_res_cleanup(lc_txn_decision_res *response);
void lc_tc_lease_acquire_res_cleanup(lc_tc_lease_acquire_res *response);
void lc_tc_lease_renew_res_cleanup(lc_tc_lease_renew_res *response);
void lc_tc_lease_release_res_cleanup(lc_tc_lease_release_res *response);
void lc_tc_leader_res_cleanup(lc_tc_leader_res *response);
void lc_tc_cluster_res_cleanup(lc_tc_cluster_res *response);
void lc_tc_rm_res_cleanup(lc_tc_rm_res *response);
void lc_tc_rm_list_res_cleanup(lc_tc_rm_list_res *response);
void lc_enqueue_res_cleanup(lc_enqueue_res *response);
void lc_queue_stats_res_cleanup(lc_queue_stats_res *response);
void lc_ack_res_cleanup(lc_ack_res *response);
void lc_nack_res_cleanup(lc_nack_res *response);
void lc_extend_res_cleanup(lc_extend_res *response);
void lc_dequeue_batch_cleanup(lc_dequeue_batch_res *response);
void lc_watch_event_cleanup(lc_watch_event *event);
void lc_attachment_info_cleanup(lc_attachment_info *info);
void lc_attach_res_cleanup(lc_attach_res *response);
void lc_attachment_list_cleanup(lc_attachment_list *response);
void lc_attachment_get_res_cleanup(lc_attachment_get_res *response);

/** Acquires a lease and returns a bound `lc_lease` handle for follow-up work.
 */
int lc_acquire(lc_client *client, const lc_acquire_req *req, lc_lease **out,
               lc_error *error);
/** Describes the current lease and state metadata for a key. */
int lc_describe(lc_client *client, const lc_describe_req *req,
                lc_describe_res *out, lc_error *error);
/** Streams a state document into `dst`. Prefer the lease method once acquired.
 */
int lc_get(lc_client *client, const char *key, const lc_get_opts *opts,
           lc_sink *dst, lc_get_res *out, lc_error *error);
/** Convenience `get()` variant that materializes the state into memory. */
int lc_load(lc_client *client, const char *key, const lc_get_opts *opts,
            char **json_text, size_t *json_length, lc_get_res *out,
            lc_error *error);
/** Updates a lease reference from a streamed JSON source. */
int lc_update(lc_client *client, const lc_update_req *req, lc_json *json,
              lc_update_res *out, lc_error *error);
/** Applies one or more server-side mutations to a lease reference. */
int lc_mutate(lc_client *client, const lc_mutate_op *req, lc_mutate_res *out,
              lc_error *error);
/** Updates metadata fields on a lease reference. */
int lc_metadata(lc_client *client, const lc_metadata_op *req,
                lc_metadata_res *out, lc_error *error);
/** Removes state bytes for a lease reference while keeping the lease alive. */
int lc_remove(lc_client *client, const lc_remove_op *req, lc_remove_res *out,
              lc_error *error);
/** Renews an existing lease reference without a bound `lc_lease` handle. */
int lc_keepalive(lc_client *client, const lc_keepalive_op *req,
                 lc_keepalive_res *out, lc_error *error);
/** Releases an existing lease reference without a bound `lc_lease` handle. */
int lc_release(lc_client *client, const lc_release_op *req, lc_release_res *out,
               lc_error *error);
/** Streams an attachment upload for a lease reference. */
int lc_attach(lc_client *client, const lc_attach_op *req, lc_source *src,
              lc_attach_res *out, lc_error *error);
/** Lists attachments associated with a lease reference. */
int lc_list_attachments(lc_client *client, const lc_attachment_list_req *req,
                        lc_attachment_list *out, lc_error *error);
/** Streams an attachment download into `dst`. */
int lc_get_attachment(lc_client *client, const lc_attachment_get_op *req,
                      lc_sink *dst, lc_attachment_get_res *out,
                      lc_error *error);
/** Deletes one attachment selected by name or digest. */
int lc_delete_attachment(lc_client *client, const lc_attachment_delete_op *req,
                         int *deleted, lc_error *error);
/** Deletes all attachments for the given lease reference. */
int lc_delete_all_attachments(lc_client *client,
                              const lc_attachment_delete_all_op *req,
                              int *deleted_count, lc_error *error);
/** Returns queue depth and delivery metadata. */
int lc_queue_stats(lc_client *client, const lc_queue_stats_req *req,
                   lc_queue_stats_res *out, lc_error *error);
/** Acknowledges a queue message from explicit client-level identifiers. */
int lc_queue_ack(lc_client *client, const lc_ack_op *req, lc_ack_res *out,
                 lc_error *error);
/**
 * Negatively acknowledges a queue message from explicit identifiers.
 *
 * `req->intent` controls whether the server records a processing failure or an
 * intentional deferral.
 */
int lc_queue_nack(lc_client *client, const lc_nack_op *req, lc_nack_res *out,
                  lc_error *error);
/** Extends queue message visibility from explicit client-level identifiers. */
int lc_queue_extend(lc_client *client, const lc_extend_op *req,
                    lc_extend_res *out, lc_error *error);
/** Streams query results into `dst`. */
int lc_query(lc_client *client, const lc_query_req *req, lc_sink *dst,
             lc_query_res *out, lc_error *error);
/** Reads namespace-level engine configuration. */
int lc_get_namespace_config(lc_client *client,
                            const lc_namespace_config_req *req,
                            lc_namespace_config_res *out, lc_error *error);
/** Updates namespace-level engine configuration. */
int lc_update_namespace_config(lc_client *client,
                               const lc_namespace_config_req *req,
                               lc_namespace_config_res *out, lc_error *error);
/** Triggers an index flush for the selected namespace. */
int lc_flush_index(lc_client *client, const lc_index_flush_req *req,
                   lc_index_flush_res *out, lc_error *error);
/** Replays a transaction by transaction identifier. */
int lc_txn_replay(lc_client *client, const lc_txn_replay_req *req,
                  lc_txn_replay_res *out, lc_error *error);
/** Prepares a transaction decision. */
int lc_txn_prepare(lc_client *client, const lc_txn_decision_req *req,
                   lc_txn_decision_res *out, lc_error *error);
/** Commits a transaction decision. */
int lc_txn_commit(lc_client *client, const lc_txn_decision_req *req,
                  lc_txn_decision_res *out, lc_error *error);
/** Rolls back a transaction decision. */
int lc_txn_rollback(lc_client *client, const lc_txn_decision_req *req,
                    lc_txn_decision_res *out, lc_error *error);
/** Acquires a TC lease. */
int lc_tc_lease_acquire(lc_client *client, const lc_tc_lease_acquire_req *req,
                        lc_tc_lease_acquire_res *out, lc_error *error);
/** Renews a TC lease. */
int lc_tc_lease_renew(lc_client *client, const lc_tc_lease_renew_req *req,
                      lc_tc_lease_renew_res *out, lc_error *error);
/** Releases a TC lease. */
int lc_tc_lease_release(lc_client *client, const lc_tc_lease_release_req *req,
                        lc_tc_lease_release_res *out, lc_error *error);
/** Returns current TC leader information. */
int lc_tc_leader(lc_client *client, lc_tc_leader_res *out, lc_error *error);
/** Announces the current node into the TC cluster set. */
int lc_tc_cluster_announce(lc_client *client,
                           const lc_tc_cluster_announce_req *req,
                           lc_tc_cluster_res *out, lc_error *error);
/** Removes the current node from the TC cluster set. */
int lc_tc_cluster_leave(lc_client *client, lc_tc_cluster_res *out,
                        lc_error *error);
/** Lists the current TC cluster set. */
int lc_tc_cluster_list(lc_client *client, lc_tc_cluster_res *out,
                       lc_error *error);
/** Registers a TC resource manager. */
int lc_tc_rm_register(lc_client *client, const lc_tc_rm_register_req *req,
                      lc_tc_rm_res *out, lc_error *error);
/** Unregisters a TC resource manager. */
int lc_tc_rm_unregister(lc_client *client, const lc_tc_rm_unregister_req *req,
                        lc_tc_rm_res *out, lc_error *error);
/** Lists registered TC resource managers. */
int lc_tc_rm_list(lc_client *client, lc_tc_rm_list_res *out, lc_error *error);
/** Streams a queue payload upload and enqueues it. */
int lc_enqueue(lc_client *client, const lc_enqueue_req *req, lc_source *src,
               lc_enqueue_res *out, lc_error *error);
/** Dequeues a message and returns an `lc_message` handle for follow-up calls.
 */
int lc_dequeue(lc_client *client, const lc_dequeue_req *req, lc_message **out,
               lc_error *error);
/** Dequeues up to `req->page_size` messages and returns them as a batch. */
int lc_dequeue_batch(lc_client *client, const lc_dequeue_req *req,
                     lc_dequeue_batch_res *out, lc_error *error);
/** Dequeues a message and includes an associated state lease handle. */
int lc_dequeue_with_state(lc_client *client, const lc_dequeue_req *req,
                          lc_message **out, lc_error *error);
/** Consumes queue messages with a streaming callback. */
int lc_subscribe(lc_client *client, const lc_dequeue_req *req,
                 const lc_consumer *consumer, lc_error *error);
/** Consumes queue messages with attached state leases. */
int lc_subscribe_with_state(lc_client *client, const lc_dequeue_req *req,
                            const lc_consumer *consumer, lc_error *error);
/**
 * Creates a managed consumer service.
 *
 * Use the returned handle for the long-running worker-service model. `run()`
 * blocks until the service stops; `start()`/`stop()`/`wait()` provide explicit
 * control when embedding into an existing daemon.
 */
int lc_client_new_consumer_service(lc_client *client,
                                   const lc_consumer_service_config *config,
                                   lc_consumer_service **out, lc_error *error);
/** Watches queue depth changes with a streaming watch callback. */
int lc_watch_queue(lc_client *client, const lc_watch_queue_req *req,
                   const lc_watch_handler *handler, lc_error *error);

/** Refreshes the published fields on a bound lease handle. */
int lc_lease_describe(lc_lease *lease, lc_error *error);
/** Streams the state document for a bound lease into `dst`. */
int lc_lease_get(lc_lease *lease, lc_sink *dst, const lc_get_opts *opts,
                 lc_get_res *out, lc_error *error);
/** Convenience `get()` variant that materializes a bound lease state in memory.
 */
int lc_lease_load(lc_lease *lease, char **json_text, size_t *json_length,
                  const lc_get_opts *opts, lc_get_res *out, lc_error *error);
/** Convenience `update()` variant for small in-memory JSON documents. */
int lc_lease_save(lc_lease *lease, const char *json_text, lc_error *error);
/** Streams a replacement JSON state document into a bound lease. */
int lc_lease_update(lc_lease *lease, lc_json *json, const lc_update_opts *opts,
                    lc_error *error);
/** Applies server-side mutations to the current state of a bound lease. */
int lc_lease_mutate(lc_lease *lease, const lc_mutate_req *req, lc_error *error);
/** Streams the current state through the local mutate engine and updates it. */
int lc_lease_mutate_local(lc_lease *lease, const lc_mutate_local_req *req,
                          lc_error *error);
/** Updates metadata fields on a bound lease. */
int lc_lease_metadata(lc_lease *lease, const lc_metadata_req *req,
                      lc_error *error);
/** Removes the current state bytes while keeping the bound lease alive. */
int lc_lease_remove(lc_lease *lease, const lc_remove_req *req, lc_error *error);
/** Renews a bound lease and refreshes its expiry fields. */
int lc_lease_keepalive(lc_lease *lease, const lc_keepalive_req *req,
                       lc_error *error);
/** Releases a bound lease and closes the handle on success. */
int lc_lease_release(lc_lease *lease, const lc_release_req *req,
                     lc_error *error);
/** Streams an attachment upload onto a bound lease. */
int lc_lease_attach(lc_lease *lease, const lc_attach_req *req, lc_source *src,
                    lc_attach_res *out, lc_error *error);
/** Lists the attachments currently associated with a bound lease. */
int lc_lease_list_attachments(lc_lease *lease, lc_attachment_list *out,
                              lc_error *error);
/** Streams an attachment download from a bound lease into `dst`. */
int lc_lease_get_attachment(lc_lease *lease, const lc_attachment_get_req *req,
                            lc_sink *dst, lc_attachment_get_res *out,
                            lc_error *error);
/** Deletes one attachment associated with a bound lease. */
int lc_lease_delete_attachment(lc_lease *lease,
                               const lc_attachment_selector *selector,
                               int *deleted, lc_error *error);
/** Deletes all attachments associated with a bound lease. */
int lc_lease_delete_all_attachments(lc_lease *lease, int *deleted_count,
                                    lc_error *error);

/** Acknowledges a bound message and closes the handle on success. */
int lc_message_ack(lc_message *message, lc_error *error);
/**
 * Negatively acknowledges a bound message and closes the handle on success.
 *
 * `req->intent` controls whether the server records a processing failure or an
 * intentional deferral.
 */
int lc_message_nack(lc_message *message, const lc_nack_req *req,
                    lc_error *error);
/** Extends visibility for a bound message without closing it. */
int lc_message_extend(lc_message *message, const lc_extend_req *req,
                      lc_error *error);
/** Returns the state lease associated with a bound message, if any. */
lc_lease *lc_message_state(lc_message *message);
/** Returns the rewindable payload reader owned by the message handle. */
lc_source *lc_message_payload(lc_message *message);
/** Rewinds a bound message payload so it can be consumed again. */
int lc_message_rewind_payload(lc_message *message, lc_error *error);
/** Copies a bound message payload into `dst`. */
int lc_message_write_payload(lc_message *message, lc_sink *dst, size_t *written,
                             lc_error *error);
/** Starts all managed consumer loops and blocks until they stop. */
int lc_consumer_service_run(lc_consumer_service *service, lc_error *error);
/** Starts all managed consumer loops asynchronously. */
int lc_consumer_service_start(lc_consumer_service *service, lc_error *error);
/** Requests shutdown of a running managed consumer service. */
int lc_consumer_service_stop(lc_consumer_service *service);
/** Waits for a managed consumer service to stop. */
int lc_consumer_service_wait(lc_consumer_service *service, lc_error *error);
/** Closes and frees a managed consumer service. */
void lc_consumer_service_close(lc_consumer_service *service);

#endif
