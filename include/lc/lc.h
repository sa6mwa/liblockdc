#ifndef LC_LOCKDC_H
#define LC_LOCKDC_H

/**
 * @file lc/lc.h
 * @brief Public C API for remote lockd and local Pouch clients.
 *
 * The API is C89-compatible and uses receiver-style handles for normal
 * workflows. Initialize transparent request/config structs with their
 * matching `*_init()` function, then call methods such as
 * `client->acquire(client, ...)` and `lease->update(lease, ...)`.
 *
 * Unless a field says otherwise, input strings, arrays, sources, sinks, and
 * callback contexts are borrowed for the duration of the call. Successful
 * operations that return handles or heap-backed response fields transfer
 * ownership to the caller. Close handles with their receiver `close()` method
 * or the matching `lc_*_close()` wrapper, and release response fields with the
 * matching `*_cleanup()` helper. The free-function close wrappers and cleanup
 * helpers accept `NULL`.
 *
 * Fallible functions return `LC_OK` on success and an `LC_ERR_*` status on
 * failure. When supplied, `lc_error` receives actionable diagnostics and must
 * eventually be passed to `lc_error_cleanup()`. Unless documented as a
 * terminal operation, an existing handle remains owned by the caller after a
 * failed method call.
 *
 * APIs described as streaming move data through bounded buffers without
 * materializing the complete value. In-memory sources/sinks and ordinary
 * mapped lonejson fields are materialized by design; lonejson source-backed
 * and spool-backed mappings retain their documented streaming/file-backed
 * behavior.
 */

#include <lc/version.h>
#include <lonejson.h>
#include <pslog.h>
#include <stddef.h>
#include <stdint.h>

/** Default maximum bytes accepted while parsing a typed JSON HTTP response. */
#define LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT (100UL * 1024UL * 1024UL)

/** Opaque client handle. */
typedef struct lc_client lc_client;
/** Opaque lease/state handle returned from acquire and message state flows. */
typedef struct lc_lease lc_lease;
/** Opaque queue message handle returned from dequeue and subscription flows. */
typedef struct lc_message lc_message;
/** Opaque managed queue consumer service. */
typedef struct lc_consumer_service lc_consumer_service;
/** Opaque byte source used for uploads and streamed request bodies. */
typedef struct lc_source lc_source;
/** Opaque byte sink used for downloads and streamed response bodies. */
typedef struct lc_sink lc_sink;

/** API-visible monotonic object version. Mirrors lockd's signed int64 value. */
typedef int64_t lc_version;
/**
 * Unix timestamp in seconds. Mirrors lockd's signed int64 value.
 *
 * Pouch and transport response paths preserve this range, including streamed
 * attachment metadata headers.
 */
typedef int64_t lc_unix_seconds;
/** Query/index sequence. Mirrors lockd's unsigned uint64 value. */
typedef uint64_t lc_index_seq;
/** Transaction-coordinator leader term. Mirrors lockd's unsigned uint64 value.
 */
typedef uint64_t lc_tc_term;

/** Snapshot fetched by `acquire_for_update()` before invoking the handler. */
typedef struct lc_state_snapshot {
  /** Streamed private state payload. `NULL` when `has_state` is zero. */
  lc_source *reader;
  /** Non-zero when the key currently has a committed state document. */
  int has_state;
  /** State content type returned by lockd. */
  const char *content_type;
  /** State entity tag returned by lockd. */
  const char *etag;
  /** State version returned by lockd. */
  lc_version version;
  /** Current fencing token returned by lockd. */
  long fencing_token;
  /** Correlation id for the snapshot read. */
  const char *correlation_id;
} lc_state_snapshot;

/** Context passed to `lc_acquire_for_update_handler_fn`. */
typedef struct lc_acquire_for_update_context {
  /**
   * Active lease handle. Borrowed for the duration of the callback; do not
   * close or release it from the handler because the helper releases it.
   */
  lc_lease *lease;
  /** Private state snapshot fetched before the callback. */
  lc_state_snapshot state;
} lc_acquire_for_update_context;

/**
 * Structured error returned by all public operations.
 *
 * The library allocates any string members it populates. Release them with
 * `lc_error_cleanup()` when the error is no longer needed.
 */
typedef struct lc_error {
  /** Public `LC_*` status code for the failed operation. */
  int code;
  /** HTTP status from the remote server, or zero for local/Pouch failures. */
  long http_status;
  /** Human-readable summary. Owned by the error object. */
  char *message;
  /** Optional longer diagnostic detail. Owned by the error object. */
  char *detail;
  /** Optional remote server error code. Owned by the error object. */
  char *server_code;
  /** Optional request correlation id. Owned by the error object. */
  char *correlation_id;
} lc_error;

/**
 * Callback invoked while an acquire-for-update lease is held.
 *
 * `update` is borrowed and valid only for this invocation. Return `LC_OK` to
 * commit staged work or a non-`LC_OK` status to request rollback. Populate
 * `error` with the failure cause when returning an error status.
 */
typedef int (*lc_acquire_for_update_handler_fn)(
    void *context, lc_acquire_for_update_context *update, lc_error *error);

/**
 * Callback used by `lc_source_from_callbacks()` to fill `buffer`.
 *
 * Write at most `count` bytes and return the number written. Short reads are
 * allowed. Return zero only for end-of-stream or failure; distinguish failure
 * by setting `error->code` to a non-`LC_OK` value. `buffer` is borrowed for the
 * call and must not be retained.
 */
typedef size_t (*lc_source_read_fn)(void *context, void *buffer, size_t count,
                                    lc_error *error);
/**
 * Optional callback used by `lc_source_from_callbacks()` to rewind a source.
 *
 * Return `LC_OK` after resetting the next read to the beginning, or a
 * non-`LC_OK` status with `error` populated. Omitting this callback makes the
 * source single-pass.
 */
typedef int (*lc_source_reset_fn)(void *context, lc_error *error);
/**
 * Optional callback invoked exactly once when a callback source is closed.
 *
 * Use it to release resources owned by `context`; it cannot report an error.
 */
typedef void (*lc_source_close_fn)(void *context);

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
  /** Allocates `size` bytes, or returns `NULL` on failure. */
  lc_malloc_fn malloc_fn;
  /** Resizes `ptr` to `size` bytes using the same allocation domain. */
  lc_realloc_fn realloc_fn;
  /** Releases memory returned by `malloc_fn` or `realloc_fn`. */
  lc_free_fn free_fn;
  /** Opaque caller context passed to every allocator hook. */
  void *context;
} lc_allocator;

/**
 * Client construction settings.
 *
 * Use `lc_client_config_init()` before overriding individual fields.
 * Set either `endpoints` for TCP/TLS operation, one `pouch://` endpoint for
 * local Pouch storage, or `unix_socket_path` for Unix-domain-socket transport.
 *
 * Pouch endpoints use an absolute filesystem root path:
 * `pouch:///var/lib/app/lockd-root`. Local Pouch storage defaults to exclusive
 * single-writer mode. A second default writer for the same root fails at open;
 * callers that intentionally need multiple active local writers must opt in
 * with `?single_writer=false` or `?pouch_single_writer=false`.
 *
 * Endpoint query options are copied at open and use the same C-native Pouch
 * storage engine as direct Pouch callers. Supported options include
 * `compression`/`pouch_compression`, `pouch_crypto_key`,
 * `pouch_crypto_key_file`, `pouch_crypto_generate_key_file`,
 * `durable_sync`, `fsync_batch_max_ops`, `segment_target_bytes`,
 * `indexer_flush_docs`, `indexer_flush_interval_seconds`,
 * `background_compaction`, `disable_compaction_throttling`,
 * `retention_seconds`, `janitor_interval_seconds`, `queue_watch`,
 * `query_engine`, and `query_fallback_engine`.
 */
typedef struct lc_client_config {
  /**
   * Endpoint URLs tried in order for TCP/TLS transport.
   *
   * For Pouch, provide exactly one `pouch://` endpoint. Pouch does not fail
   * over across multiple roots through this field.
   */
  const char *const *endpoints;
  /** Number of entries in `endpoints`. */
  size_t endpoint_count;
  /** Unix-domain socket path used instead of `endpoints` when set. */
  const char *unix_socket_path;
  /** Streamed combined CA/client-cert/private-key PEM bundle.
   *
   * Preferred over `client_bundle_path`. The source is borrowed and consumed
   * during `lc_client_open()`; callers remain responsible for closing it.
   */
  lc_source *client_bundle_source;
  /** Deprecated compatibility path to the combined PEM bundle. */
  const char *client_bundle_path;
  /** Namespace used when a request leaves `namespace_name` unset. */
  const char *default_namespace;
  /**
   * Whole-request timeout in milliseconds.
   *
   * Remote requests use this as the transport timeout. Local Pouch operations
   * remain local filesystem calls; blocking lease/dequeue behavior is
   * controlled by the corresponding request fields.
   */
  long timeout_ms;
  /** Disables mTLS client authentication when the server allows it. */
  int disable_mtls;
  /** Skips peer verification. Intended for local dev and test only. */
  int insecure_skip_verify;
  /** Prefers HTTP/2 when the endpoint and libcurl build support it. */
  int prefer_http_2;
  /** Maximum typed JSON response bytes parsed through lonejson. Zero uses
   * `LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT`.
   */
  size_t http_json_response_limit_bytes;
  /** Borrowed client logger used for SDK diagnostics. Defaults to a no-op
   * logger. */
  pslog_logger *logger;
  /** Disables the automatic `sys=client.lockd` logger field derived by the
   * SDK when non-zero. */
  int disable_logger_sys_field;
  /** Custom allocator hooks inherited by all derived handles and buffers. */
  lc_allocator allocator;
  /**
   * Pouch root key string for encrypted local storage.
   *
   * Prefer key files for long-lived process config. This string is copied as
   * secret material and wiped before the client releases it. It may also be
   * supplied as the `pouch_crypto_key` endpoint option.
   */
  const char *pouch_crypto_key;
  /**
   * Pouch root key file path for encrypted local storage.
   *
   * The file stores an `lc-pouch-key-v1:<base64url>` root key. It may also be
   * supplied as the `pouch_crypto_key_file` endpoint option.
   */
  const char *pouch_crypto_key_file;
  /**
   * Creates `pouch_crypto_key_file` with a new root key when it is missing.
   *
   * Generation writes a mode-0600 file below mode-0700 parent directories.
   */
  int pouch_crypto_generate_key_file;
  /** Local Pouch at-rest compression mode: NULL/"none" disables it, "zlib"
   * enables streaming zlib compression before storage/encryption. Consumer
   * service workers retain the resolved mode when reopening this Pouch root.
   */
  const char *pouch_compression;
} lc_client_config;

/** Public status codes returned by all API entry points. */
enum {
  /** Operation completed successfully. */
  LC_OK = 0,
  /** A required argument, value, state, or precondition is invalid. */
  LC_ERR_INVALID = 1,
  /** Allocation failed. */
  LC_ERR_NOMEM = 2,
  /** Transport, filesystem, or other I/O failed. */
  LC_ERR_TRANSPORT = 3,
  /** A remote response or durable local value violated its protocol. */
  LC_ERR_PROTOCOL = 4,
  /** The lockd server rejected an otherwise valid request. */
  LC_ERR_SERVER = 5
};

/**
 * Input stream used for uploads and generic byte transport. A source may be
 * single-pass; in that case `reset()` returns `LC_ERR_INVALID`.
 *
 * The source owns `impl` and releases it from `close()`.
 */
struct lc_source {
  /** Reads up to `count` bytes into `buffer`; returns zero at EOF. */
  size_t (*read)(lc_source *self, void *buffer, size_t count, lc_error *error);
  /** Rewinds the source when supported; otherwise returns `LC_ERR_INVALID`. */
  int (*reset)(lc_source *self, lc_error *error);
  /** Releases the source and any owned backing resources. */
  void (*close)(lc_source *self);
  /** Private implementation pointer owned by the source. */
  void *impl;
};

/**
 * Output stream used for downloads and generic byte transport.
 *
 * The sink owns `impl` and releases it from `close()`.
 */
struct lc_sink {
  /**
   * Writes exactly `count` bytes.
   *
   * Returns non-zero on success or zero with `error` populated on failure.
   * `bytes` is borrowed for the call and must not be retained.
   */
  int (*write)(lc_sink *self, const void *bytes, size_t count, lc_error *error);
  /** Releases the sink and any owned backing resources. */
  void (*close)(lc_sink *self);
  /** Private implementation pointer owned by the sink. */
  void *impl;
};

/**
 * Callback used to resolve a file-backed local-mutate input into a new source.
 *
 * `resolved_path` is borrowed for the call. Return `LC_OK` and transfer a new
 * source through `out`, or return a non-`LC_OK` status with `error` populated.
 * The local mutate operation closes a successfully returned source.
 */
typedef int (*lc_file_value_open_fn)(void *context, const char *resolved_path,
                                     lc_source **out, lc_error *error);

/** Optional override used by local mutate to open file-backed values. */
typedef struct lc_file_value_resolver {
  /** Opens `resolved_path` and transfers the returned source to the caller. */
  lc_file_value_open_fn open;
  /** Opaque borrowed context passed to `open`. */
  void *context;
} lc_file_value_resolver;

/** Request used to acquire a new lease. */
typedef struct lc_acquire_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *namespace_name;
  /** Lease key to acquire. */
  const char *key;
  /** Non-empty logical owner identifier required for lease acquisition. */
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
  /** State version visible to the acquired lease. */
  lc_version version;
  /** Private state entity tag for the acquired lease. */
  char *state_etag;
  /** Fencing token assigned to this lease acquisition. */
  long fencing_token;
  /** Server correlation id for the acquisition. */
  char *correlation_id;
} lc_acquire_res;

/** Stable lease identity used by client-level operations on an existing lease.
 * Mutating operations require a non-empty `lease_id` and its matching fencing
 * token; an empty reference is never an unfenced write request. */
typedef struct lc_lease_ref {
  /** Namespace of the existing lease. */
  const char *namespace_name;
  /** Key of the existing lease. */
  const char *key;
  /** Server-issued lease identifier, required for every mutation. */
  const char *lease_id;
  /** Transaction identifier associated with the lease, if any. */
  const char *txn_id;
  /** Current fencing token for optimistic concurrency. */
  long fencing_token;
} lc_lease_ref;

/** Request used to describe a lease or state object. */
typedef struct lc_describe_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *namespace_name;
  /** Key to describe. */
  const char *key;
} lc_describe_req;

/** Lease description returned by describe operations. */
typedef struct lc_describe_res {
  /** Namespace of the described key. */
  char *namespace_name;
  /** Described key. */
  char *key;
  /** Current lease owner, when a lease is active. */
  char *owner;
  /** Current state version. */
  lc_version version;
  /** Current lease identifier, when a lease is active. */
  char *lease_id;
  /** Current lease expiry as a Unix timestamp, or zero when not leased. */
  lc_unix_seconds lease_expires_at_unix;
  /** Current fencing token, or zero when not leased. */
  long fencing_token;
  /** Transaction identifier associated with the lease, if any. */
  char *txn_id;
  /** Private state etag. */
  char *state_etag;
  /** Public committed state etag. */
  char *public_state_etag;
  /** Non-zero when `query_hidden` was explicitly set. */
  int has_query_hidden;
  /** Whether the public state is hidden from query results. */
  int query_hidden;
  /** Server correlation id for the describe response. */
  char *correlation_id;
} lc_describe_res;

/** Optional flags that control state reads. */
typedef struct lc_get_opts {
  /** Reads the public view instead of the private lease-bound state. */
  int public_read;
} lc_get_opts;

/** Metadata returned by streamed or buffered state reads. */
typedef struct lc_get_res {
  /** Non-zero when the key has no readable state body. */
  int no_content;
  /** Content type recorded for the returned state body. */
  char *content_type;
  /** Entity tag for the returned state body. */
  char *etag;
  /** Version of the returned state body. */
  lc_version version;
  /** Fencing token associated with the lease-bound read, if any. */
  long fencing_token;
  /** Server correlation id for the read response. */
  char *correlation_id;
} lc_get_res;

/** Optional controls for streamed JSON state updates. */
typedef struct lc_update_opts {
  /** Optional state etag precondition for optimistic concurrency. */
  const char *if_state_etag;
  /** Optional version precondition for optimistic concurrency. */
  lc_version if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
  /** Content type sent with the JSON state body. */
  const char *content_type;
} lc_update_opts;

/** Client-level update operation for an existing, credentialed lease reference.
 */
typedef struct lc_update_req {
  /** Existing lease identity to update. */
  lc_lease_ref lease;
  /** Optional state etag precondition for optimistic concurrency. */
  const char *if_state_etag;
  /** Optional version precondition for optimistic concurrency. */
  lc_version if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
  /** Content type sent with the JSON state body. */
  const char *content_type;
} lc_update_req;

/** Metadata returned by a successful state update. */
typedef struct lc_update_res {
  /** State version after the update. */
  lc_version new_version;
  /** Owned entity tag for the updated private state. */
  char *new_state_etag;
  /** State bytes, representable by `long` on the calling architecture. */
  long bytes;
  /** Owned server correlation id for the update response. */
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
  lc_version if_version;
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

/** Client-level mutate operation for an existing, credentialed lease reference.
 */
typedef struct lc_mutate_op {
  /** Existing lease identity to mutate. */
  lc_lease_ref lease;
  /** JSON mutation expressions evaluated in order by the server. */
  const char *const *mutations;
  /** Number of entries in `mutations`. */
  size_t mutation_count;
  /** Optional state etag precondition for optimistic concurrency. */
  const char *if_state_etag;
  /** Optional version precondition for optimistic concurrency. */
  lc_version if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
} lc_mutate_op;

/** Metadata returned by a successful mutate operation. */
typedef struct lc_mutate_res {
  /** State version after applying all mutations. */
  lc_version new_version;
  /** Owned entity tag for the mutated private state. */
  char *new_state_etag;
  /** State bytes, representable by `long` on the calling architecture. */
  long bytes;
  /** Owned server correlation id for the mutate response. */
  char *correlation_id;
} lc_mutate_res;

/** Request used to update lease metadata without replacing state bytes. */
typedef struct lc_metadata_req {
  /** Set to non-zero when `query_hidden` should be updated. */
  int has_query_hidden;
  /** Whether the state should be hidden from normal query results. */
  int query_hidden;
  /** Optional version precondition for optimistic concurrency. */
  lc_version if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
} lc_metadata_req;

/** Client-level metadata update operation for an existing, credentialed lease
 * reference. */
typedef struct lc_metadata_op {
  /** Existing lease identity to update. */
  lc_lease_ref lease;
  /** Set to non-zero when `query_hidden` should be updated. */
  int has_query_hidden;
  /** Whether the state should be hidden from normal query results. */
  int query_hidden;
  /** Optional version precondition for optimistic concurrency. */
  lc_version if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
} lc_metadata_op;

/** Metadata returned by a successful metadata update. */
typedef struct lc_metadata_res {
  /** Namespace of the updated key. */
  char *namespace_name;
  /** Updated key. */
  char *key;
  /** State version after the metadata update. */
  lc_version version;
  /** Non-zero when `query_hidden` is set. */
  int has_query_hidden;
  /** Current query visibility flag. */
  int query_hidden;
  /** Server correlation id for the metadata response. */
  char *correlation_id;
} lc_metadata_res;

/** Request used to remove state bytes while keeping the lease. */
typedef struct lc_remove_req {
  /** Optional state etag precondition for optimistic concurrency. */
  const char *if_state_etag;
  /** Optional version precondition for optimistic concurrency. */
  lc_version if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
} lc_remove_req;

/** Client-level remove operation for an existing, credentialed lease reference.
 */
typedef struct lc_remove_op {
  /** Existing lease identity to remove state from. */
  lc_lease_ref lease;
  /** Optional state etag precondition for optimistic concurrency. */
  const char *if_state_etag;
  /** Optional version precondition for optimistic concurrency. */
  lc_version if_version;
  /** Enables `if_version` when non-zero so version `0` stays representable. */
  int has_if_version;
} lc_remove_op;

/** Result returned by a remove operation. */
typedef struct lc_remove_res {
  /** Non-zero when state bytes were removed. */
  int removed;
  /** State version after removal. */
  lc_version new_version;
  /** Owned server correlation id for the remove response. */
  char *correlation_id;
} lc_remove_res;

/** Request used to extend an existing lease. */
typedef struct lc_keepalive_req {
  /** New TTL in seconds for the renewed lease. */
  long ttl_seconds;
} lc_keepalive_req;

/** Client-level keepalive operation for an existing lease reference. */
typedef struct lc_keepalive_op {
  /** Existing lease identity to renew. */
  lc_lease_ref lease;
  /** New TTL in seconds for the renewed lease. */
  long ttl_seconds;
} lc_keepalive_op;

/** Result returned by a successful keepalive operation. */
typedef struct lc_keepalive_res {
  /** New lease expiry as a Unix timestamp. */
  lc_unix_seconds lease_expires_at_unix;
  /** State version visible after renewal. */
  lc_version version;
  /** Private state etag visible after renewal. */
  char *state_etag;
  /** Server correlation id for the keepalive response. */
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
  /** Existing lease identity to release. */
  lc_lease_ref lease;
  /** Rolls transactional state back before releasing when non-zero. */
  int rollback;
} lc_release_op;

/** Result returned by a release operation. */
typedef struct lc_release_res {
  /** Non-zero when the durable lease was released. */
  int released;
  /** Owned server correlation id for the release response. */
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
  /** Optional query engine hint, for example `index` or `scan`. */
  const char *engine;
  /** Optional query refresh mode, for example `wait_for`. */
  const char *refresh;
  /**
   * Full-form LQL selector expression used by the query engine. Mutually
   * exclusive with `selector_json`.
   */
  const char *selector_lql;
} lc_query_req;

/** Query metadata returned alongside streamed query results. */
typedef struct lc_query_res {
  /** Cursor for the next result page, or `NULL` when there is no next page. */
  char *cursor;
  /** Return mode selected by the engine for this response. */
  char *return_mode;
  /** Query/index freshness sequence observed by this response. */
  lc_index_seq index_seq;
  /** Server correlation id for the query response. */
  char *correlation_id;
  /** Raw JSON metadata emitted by query responses, when present. */
  char *metadata_json;
} lc_query_res;

/** Chunked callback handler for `lc_query_keys()`. */
typedef struct lc_query_key_handler {
  /** Called when a decoded key string begins; return `LC_OK` to continue. */
  int (*begin)(void *context, lc_error *error);
  /**
   * Called with decoded UTF-8 key bytes; return `LC_OK` to continue.
   *
   * Bytes are borrowed and valid only for the call. A key may arrive in
   * multiple chunks, including chunks that split a UTF-8 sequence.
   */
  int (*chunk)(void *context, const char *bytes, size_t len, lc_error *error);
  /**
   * Called after the key and following array delimiter are validated.
   * Return `LC_OK` to continue with the next key.
   */
  int (*end)(void *context, lc_error *error);
} lc_query_key_handler;

/** Generic owned string list used by several management responses. */
typedef struct lc_string_list {
  /** Owned array of owned strings. */
  char **items;
  /** Number of entries in `items`. */
  size_t count;
} lc_string_list;

/** Request used to read or update namespace configuration. */
typedef struct lc_namespace_config_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *namespace_name;
  /** Preferred query engine, for example `index` or `scan`. */
  const char *preferred_engine;
  /** Fallback query engine, for example `scan`. */
  const char *fallback_engine;
  /** Optional etag precondition for updates. */
  const char *if_etag;
} lc_namespace_config_req;

/** Namespace configuration returned by get or update operations. */
typedef struct lc_namespace_config_res {
  /** Namespace whose config was returned. */
  char *namespace_name;
  /** Preferred query engine. */
  char *preferred_engine;
  /** Fallback query engine. */
  char *fallback_engine;
  /** Namespace config etag. */
  char *etag;
  /** Server correlation id for the namespace config response. */
  char *correlation_id;
} lc_namespace_config_res;

/** Request used to flush an index for a namespace. */
typedef struct lc_index_flush_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *namespace_name;
  /** Flush mode, for example `wait` for synchronous freshness. */
  const char *mode;
} lc_index_flush_req;

/** Result returned by an index flush request. */
typedef struct lc_index_flush_res {
  /** Namespace whose index was flushed. */
  char *namespace_name;
  /** Flush mode applied by the engine. */
  char *mode;
  /** Engine-generated flush identifier, when present. */
  char *flush_id;
  /** Non-zero when an asynchronous flush was accepted. */
  int accepted;
  /** Non-zero when the index was flushed before returning. */
  int flushed;
  /** Non-zero when index work remains pending. */
  int pending;
  /** Index sequence visible after the flush request. */
  lc_index_seq index_seq;
  /** Server correlation id for the flush response. */
  char *correlation_id;
} lc_index_flush_res;

/** Transaction participant identity used by TC prepare/commit/rollback
 * operations. A nonempty backend hash targets that exact resource manager;
 * an empty hash is a compatibility wildcard. Outer whitespace is normalized;
 * whitespace-only nonempty values are invalid. */
typedef struct lc_txn_participant {
  /** Participant namespace. */
  const char *namespace_name;
  /** Participant key. */
  const char *key;
  /** Target backend hash or compatibility wildcard. */
  const char *backend_hash;
} lc_txn_participant;

/** Request used to replay transaction coordinator state for a transaction id.
 */
typedef struct lc_txn_replay_req {
  /** Transaction identifier to replay. */
  const char *txn_id;
} lc_txn_replay_req;

/** Result returned by transaction replay. */
typedef struct lc_txn_replay_res {
  /** Replayed transaction identifier. */
  char *txn_id;
  /** Durable decision state after replay. */
  char *state;
  /** Server correlation id for the replay response. */
  char *correlation_id;
} lc_txn_replay_res;

/** Request used for TC prepare, commit, and rollback decisions.
 * A nonempty target_backend_hash scopes the decision to one receiving resource
 * manager. It is normalized for outer whitespace and must match that resource
 * manager's backend identity. */
typedef struct lc_txn_decision_req {
  /** Transaction identifier to decide. */
  const char *txn_id;
  /** Participant set covered by this decision. */
  const lc_txn_participant *participants;
  /** Number of entries in `participants`. */
  size_t participant_count;
  /** Expiry for a prepared decision as a Unix timestamp. */
  lc_unix_seconds expires_at_unix;
  /** Transaction-coordinator term for this decision. */
  lc_tc_term tc_term;
  /** Optional exact backend target for this decision. */
  const char *target_backend_hash;
} lc_txn_decision_req;

/** Result returned by TC prepare, commit, or rollback. */
typedef struct lc_txn_decision_res {
  /** Decided transaction identifier. */
  char *txn_id;
  /** Durable decision state after the operation. */
  char *state;
  /** Server correlation id for the decision response. */
  char *correlation_id;
} lc_txn_decision_res;

/** Request used to acquire the TC leader lease. */
typedef struct lc_tc_lease_acquire_req {
  /** Non-empty identity of the leadership candidate. */
  const char *candidate_id;
  /** Endpoint advertised for the candidate. */
  const char *candidate_endpoint;
  /** Candidate term; stale terms are rejected. */
  lc_tc_term term;
  /** Requested leader-lease TTL in milliseconds. */
  long ttl_ms;
} lc_tc_lease_acquire_req;

/** Result returned by a TC leader lease acquisition attempt. */
typedef struct lc_tc_lease_acquire_res {
  /** Non-zero when the candidate acquired leadership. */
  int granted;
  /** Owned identity of the leader observed after the request. */
  char *leader_id;
  /** Owned endpoint of the leader observed after the request. */
  char *leader_endpoint;
  /** Current transaction-coordinator term. */
  lc_tc_term term;
  /** Leader-lease expiry as a Unix timestamp. */
  lc_unix_seconds expires_at_unix;
  /** Owned server correlation id for the acquisition response. */
  char *correlation_id;
} lc_tc_lease_acquire_res;

/** Request used to renew the TC leader lease. */
typedef struct lc_tc_lease_renew_req {
  /** Identity of the current leader. */
  const char *leader_id;
  /** Term held by the current leader. */
  lc_tc_term term;
  /** Requested renewed TTL in milliseconds. */
  long ttl_ms;
} lc_tc_lease_renew_req;

/** Result returned by TC leader lease renewal. */
typedef struct lc_tc_lease_renew_res {
  /** Non-zero when the leader lease was renewed. */
  int renewed;
  /** Owned identity of the leader observed after the request. */
  char *leader_id;
  /** Owned endpoint of the leader observed after the request. */
  char *leader_endpoint;
  /** Current transaction-coordinator term. */
  lc_tc_term term;
  /** Renewed leader-lease expiry as a Unix timestamp. */
  lc_unix_seconds expires_at_unix;
  /** Owned server correlation id for the renewal response. */
  char *correlation_id;
} lc_tc_lease_renew_res;

/** Request used to release the TC leader lease. */
typedef struct lc_tc_lease_release_req {
  /** Identity of the leader releasing the lease. */
  const char *leader_id;
  /** Term held by the releasing leader. */
  lc_tc_term term;
} lc_tc_lease_release_req;

/** Result returned by TC leader lease release. */
typedef struct lc_tc_lease_release_res {
  /** Non-zero when the leader lease was released. */
  int released;
  /** Owned server correlation id for the release response. */
  char *correlation_id;
} lc_tc_lease_release_res;

/** Snapshot of the current transaction coordinator leader. */
typedef struct lc_tc_leader_res {
  /** Owned current leader identity, or `NULL` when no leader is active. */
  char *leader_id;
  /** Owned current leader endpoint, or `NULL` when no leader is active. */
  char *leader_endpoint;
  /** Current transaction-coordinator term. */
  lc_tc_term term;
  /** Current leader-lease expiry as a Unix timestamp. */
  lc_unix_seconds expires_at_unix;
  /** Owned server correlation id for the leader response. */
  char *correlation_id;
} lc_tc_leader_res;

/** Request used to announce the current node into the TC cluster. */
typedef struct lc_tc_cluster_announce_req {
  /** Endpoint advertised by this node. */
  const char *self_endpoint;
} lc_tc_cluster_announce_req;

/** Cluster membership state returned by TC cluster operations. */
typedef struct lc_tc_cluster_res {
  /** Owned list of currently registered cluster endpoints. */
  lc_string_list endpoints;
  /** Last membership update as a Unix timestamp. */
  lc_unix_seconds updated_at_unix;
  /** Membership expiry as a Unix timestamp, or zero when not exposed. */
  lc_unix_seconds expires_at_unix;
  /** Owned server correlation id for the cluster response. */
  char *correlation_id;
} lc_tc_cluster_res;

/** Request used to register a resource manager endpoint for a backend hash. */
typedef struct lc_tc_rm_register_req {
  /** Non-empty stable backend identity hash. */
  const char *backend_hash;
  /** Resource-manager endpoint to register. */
  const char *endpoint;
} lc_tc_rm_register_req;

/** Request used to unregister a resource manager endpoint for a backend hash.
 */
typedef struct lc_tc_rm_unregister_req {
  /** Stable backend identity hash. */
  const char *backend_hash;
  /** Resource-manager endpoint to unregister. */
  const char *endpoint;
} lc_tc_rm_unregister_req;

/** Result returned by TC RM register and unregister operations. */
typedef struct lc_tc_rm_res {
  /** Owned backend identity hash affected by the operation. */
  char *backend_hash;
  /** Owned current endpoint list for that backend. */
  lc_string_list endpoints;
  /** Last registry update as a Unix timestamp. */
  lc_unix_seconds updated_at_unix;
  /** Owned server correlation id for the registry response. */
  char *correlation_id;
} lc_tc_rm_res;

/** Single TC RM backend entry returned by list operations. */
typedef struct lc_tc_rm_backend {
  /** Owned backend identity hash. */
  char *backend_hash;
  /** Owned endpoints currently registered for the backend. */
  lc_string_list endpoints;
  /** Last update for this backend as a Unix timestamp. */
  lc_unix_seconds updated_at_unix;
} lc_tc_rm_backend;

/** Full TC RM backend listing. */
typedef struct lc_tc_rm_list_res {
  /** Owned array of backend registry entries. */
  lc_tc_rm_backend *backends;
  /** Number of entries in `backends`. */
  size_t backend_count;
  /** Last update for the returned registry view. */
  lc_unix_seconds updated_at_unix;
  /** Owned server correlation id for the listing response. */
  char *correlation_id;
} lc_tc_rm_list_res;

/** Request used to enqueue a streamed queue payload. */
typedef struct lc_enqueue_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *namespace_name;
  /** Queue name. */
  const char *queue;
  /** Initial delivery delay in seconds. The resulting Unix timestamp must fit
   * in `lc_unix_seconds`. */
  long delay_seconds;
  /** Visibility timeout granted to the consumer on dequeue. The resulting
   * Unix timestamp must fit in `lc_unix_seconds`. */
  long visibility_timeout_seconds;
  /** Message TTL in seconds. The resulting Unix timestamp must fit in
   * `lc_unix_seconds`. */
  long ttl_seconds;
  /** Maximum delivery attempts before the server gives up. */
  int max_attempts;
  /** Payload content type recorded with the message. */
  const char *content_type;
} lc_enqueue_req;

/** Result returned by a successful queue enqueue operation. */
typedef struct lc_enqueue_res {
  /** Owned namespace containing the message. */
  char *namespace_name;
  /** Owned queue name. */
  char *queue;
  /** Owned server-issued message identifier. */
  char *message_id;
  /** Initial delivery-attempt count. */
  int attempts;
  /** Maximum delivery attempts configured for the message. */
  int max_attempts;
  /** Initial processing-failure attempt count. */
  int failure_attempts;
  /** Earliest delivery time as a Unix timestamp. */
  lc_unix_seconds not_visible_until_unix;
  /** Returned visibility timeout. Values outside `long` are rejected. */
  long visibility_timeout_seconds;
  /** Returned payload byte count. Values outside `long` are rejected. */
  long payload_bytes;
  /** Owned server correlation id for the enqueue response. */
  char *correlation_id;
} lc_enqueue_res;

/** Request used to dequeue queue messages. */
typedef struct lc_dequeue_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *namespace_name;
  /** Queue name. */
  const char *queue;
  /**
   * Non-empty logical consumer/owner identifier required for direct delivery.
   * `lc_consumer_service` may synthesize this value from its configuration.
   */
  const char *owner;
  /** Optional transaction identifier to bind the dequeue into. */
  const char *txn_id;
  /** Visibility timeout granted when a message is delivered. The resulting
   * Unix timestamp must fit in `lc_unix_seconds`. */
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
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *namespace_name;
  /** Queue name. */
  const char *queue;
} lc_queue_stats_req;

/** Queue-level metrics and head-of-line metadata. */
typedef struct lc_queue_stats_res {
  /** Namespace containing the queue. */
  char *namespace_name;
  /** Queue name. */
  char *queue;
  /** Number of consumers currently waiting. */
  int waiting_consumers;
  /** Number of pending delivery candidates observed by the backend. */
  int pending_candidates;
  /** Total registered consumers observed by the backend. */
  int total_consumers;
  /** Non-zero when a queue watcher is active. */
  int has_active_watcher;
  /** Non-zero when a message is immediately available. */
  int available;
  /** Message id at the head of the queue, when present. */
  char *head_message_id;
  /** Enqueue timestamp for the head message. */
  lc_unix_seconds head_enqueued_at_unix;
  /** Visibility deadline for the head message. */
  lc_unix_seconds head_not_visible_until_unix;
  /** Age of the head message in seconds. */
  long head_age_seconds;
  /** Server correlation id for the stats response. */
  char *correlation_id;
} lc_queue_stats_res;

/**
 * Batch result returned by `dequeue_batch()`.
 *
 * The result owns every message until `lc_dequeue_batch_cleanup()` is called.
 * A terminal `ack()` or `nack()` on a batch message does not close that handle;
 * the batch cleanup releases it.
 */
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
  /** Non-zero when the message was acknowledged. */
  int acked;
  /** Server correlation id for the ack response. */
  char *correlation_id;
} lc_ack_res;

/** Client-level ack operation on an existing queue message reference. */
typedef struct lc_ack_op {
  /** Queue message identity to acknowledge. */
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
  /** Delay in seconds before the message becomes visible again. The resulting
   * Unix timestamp must fit in `lc_unix_seconds`. */
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
  /** Queue message identity to negatively acknowledge. */
  lc_message_ref message;
  /** Delay in seconds before the message becomes visible again. The resulting
   * Unix timestamp must fit in `lc_unix_seconds`. */
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
  /** Non-zero when the message was returned to the delivery lifecycle. */
  int requeued;
  /** Owned updated queue metadata etag. */
  char *meta_etag;
  /** Owned server correlation id for the nack response. */
  char *correlation_id;
} lc_nack_res;

/** Request used to extend queue message visibility. */
typedef struct lc_extend_req {
  /** Additional visibility time in seconds. The resulting Unix timestamp must
   * fit in `lc_unix_seconds`. */
  long extend_by_seconds;
} lc_extend_req;

/** Client-level extend operation on an existing queue message reference. */
typedef struct lc_extend_op {
  /** Queue message identity to extend. */
  lc_message_ref message;
  /** Additional visibility time in seconds. The resulting Unix timestamp must
   * fit in `lc_unix_seconds`. */
  long extend_by_seconds;
} lc_extend_op;

/** Result returned by queue visibility extension operations. */
typedef struct lc_extend_res {
  /** New delivery lease expiry as a Unix timestamp. */
  lc_unix_seconds lease_expires_at_unix;
  /** Effective visibility timeout in seconds. */
  long visibility_timeout_seconds;
  /** Updated queue metadata etag. */
  char *meta_etag;
  /** New associated state-lease expiry when state is attached. */
  lc_unix_seconds state_lease_expires_at_unix;
  /** Server correlation id for the extend response. */
  char *correlation_id;
} lc_extend_res;

/** Request used to watch queue availability changes. */
typedef struct lc_watch_queue_req {
  /** Target namespace, or `NULL` to use `client->default_namespace`. */
  const char *namespace_name;
  /** Queue name. */
  const char *queue;
} lc_watch_queue_req;

/** Event delivered to queue watch handlers. */
typedef struct lc_watch_event {
  /** Namespace containing the watched queue. */
  char *namespace_name;
  /** Queue name. */
  char *queue;
  /** Non-zero when a message is available. */
  int available;
  /** Message id at the queue head, when present. */
  char *head_message_id;
  /** Timestamp when the availability state changed. */
  lc_unix_seconds changed_at_unix;
  /** Server correlation id for the watch event. */
  char *correlation_id;
} lc_watch_event;

/**
 * Callback invoked for each queue watch event.
 *
 * `event` and all of its fields are borrowed for the invocation. Return
 * `LC_OK` to continue watching or a non-`LC_OK` status with `error` populated
 * to stop the watch.
 */
typedef int (*lc_watch_handler_fn)(void *context, const lc_watch_event *event,
                                   lc_error *error);

/** Queue watch callback registration. */
typedef struct lc_watch_handler {
  /** Required event callback; returning non-`LC_OK` stops the watch. */
  lc_watch_handler_fn handle;
  /** Opaque borrowed context passed to `handle`. */
  void *context;
} lc_watch_handler;

/**
 * Queue consumer callback registration used by subscribe flows.
 *
 * The message is borrowed for the callback. Before returning `LC_OK`, the
 * callback must terminalize it with `message->ack()` or `message->nack()`.
 * Returning an error while the message remains open causes the subscribe flow
 * to attempt a failure nack before closing the local handle.
 */
typedef struct lc_consumer {
  /** Required delivery callback; return `LC_OK` only after ack or nack. */
  int (*handle)(void *context, lc_message *message, lc_error *error);
  /** Opaque borrowed context passed to `handle`. */
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
 * may leave the delivery open and return `LC_OK`; the managed consumer will
 * then acknowledge it automatically on success. If the handler returns a
 * non-`LC_OK` error, the managed consumer treats it as a failure, negatively
 * acknowledges the open delivery after the callback unwinds, reports the
 * error, and resumes consuming without counting that delivery against the
 * service restart budget.
 * Explicit `message->ack()`/`message->nack()` remain available for handlers
 * that want to terminalize the delivery themselves. When `state` is non-NULL,
 * it is the lease handle associated with the delivery and is owned by
 * `message`.
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
 * Recoverable consumer service error reported to `on_error`.
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
  /** Current consecutive loop failure count. Zero means the error was scoped to
   * one delivery and did not consume the service restart budget. */
  int attempt;
  /** Delay before the next retry, in milliseconds. Zero means immediate retry
   * or a delivery-level error with no service-level backoff. */
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
   * Return `LC_OK` after successfully processing the delivery. If the handler
   * leaves the delivery open, the managed consumer acknowledges it
   * automatically on success and negatively acknowledges it automatically on
   * failure before reporting the delivery error and resuming consumption.
   * Explicit
   * `message->nack()`/`message->ack()` are still available when a handler wants
   * to terminalize the delivery itself before returning.
   */
  int (*handle)(void *context, lc_consumer_message *message, lc_error *error);
  /**
   * Observes a failed delivery or failed loop.
   *
   * Delivery-level errors are reported with `attempt == 0` and do not consume
   * the service restart budget. Loop/runtime failures are reported with a
   * positive `attempt` and may back off according to `restart_policy`.
   * Return `LC_OK` to continue. Returning a non-`LC_OK` code stops the service
   * and returns that error from `run()`/`wait()`.
   */
  int (*on_error)(void *context, const lc_consumer_error *event,
                  lc_error *error);
  /** Called when one consumer attempt starts. */
  void (*on_start)(void *context, const lc_consumer_lifecycle_event *event);
  /** Called when one consumer attempt stops. */
  void (*on_stop)(void *context, const lc_consumer_lifecycle_event *event);
  /** Opaque user context passed to all callbacks for this consumer. */
  void *context;
  /** Restart behavior after loop/runtime transport failures. */
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
  /** Attachment content/hash identifier. Mutually exclusive with `name`. */
  const char *id;
  /** Attachment name. Mutually exclusive with `id`. */
  const char *name;
} lc_attachment_selector;

/** Attachment metadata returned by list, fetch, and attach operations. */
typedef struct lc_attachment_info {
  /** Attachment content/hash identifier. */
  char *id;
  /** Attachment name. */
  char *name;
  /** Attachment byte count. Values outside `long` are rejected. */
  long size;
  /** Hex SHA-256 digest of the plaintext attachment bytes. */
  char *plaintext_sha256;
  /** Content type recorded for the attachment. */
  char *content_type;
  /** Unix timestamp when this attachment was first created. */
  lc_unix_seconds created_at_unix;
  /** Unix timestamp of the attachment revision returned by this operation. */
  lc_unix_seconds updated_at_unix;
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

/** Client-level attachment upload operation for an existing, credentialed
 * lease reference. */
typedef struct lc_attach_op {
  /** Existing lease identity to attach to. */
  lc_lease_ref lease;
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
} lc_attach_op;

/** Result returned by an attachment upload. */
typedef struct lc_attach_res {
  /** Metadata for the written attachment. */
  lc_attachment_info attachment;
  /** Non-zero when the request was a no-op. */
  int noop;
  /** State version after the attachment update. */
  lc_version version;
  /** Server correlation id for the attach response. */
  char *correlation_id;
} lc_attach_res;

/** Attachment listing returned by list operations. */
typedef struct lc_attachment_list {
  /** Owned array of attachment metadata entries. */
  lc_attachment_info *items;
  /** Number of entries in `items`. */
  size_t count;
  /** Server correlation id for the list response. */
  char *correlation_id;
} lc_attachment_list;

/** Request used to fetch an attachment stream. */
typedef struct lc_attachment_get_req {
  /** Attachment id or name selector. */
  lc_attachment_selector selector;
  /** Reads the committed public view when nonzero. Private reads require an
   * active lease and, when transaction-bound, include that transaction's
   * staged attachment changes. */
  int public_read;
} lc_attachment_get_req;

/** Client-level request used to list lease attachments. */
typedef struct lc_attachment_list_req {
  /** Existing lease identity to list through. */
  lc_lease_ref lease;
  /** Reads the committed public view when nonzero. Private reads require an
   * active lease and, when transaction-bound, include that transaction's
   * staged attachment changes. */
  int public_read;
} lc_attachment_list_req;

/** Client-level attachment fetch operation for an existing lease reference. */
typedef struct lc_attachment_get_op {
  /** Existing lease identity to read through when `public_read` is zero. */
  lc_lease_ref lease;
  /** Attachment id or name selector. */
  lc_attachment_selector selector;
  /** Reads the committed public view when nonzero. Private reads require an
   * active lease and, when transaction-bound, include that transaction's
   * staged attachment changes. */
  int public_read;
} lc_attachment_get_op;

/** Client-level attachment delete operation for an existing, credentialed
 * lease reference. */
typedef struct lc_attachment_delete_op {
  /** Existing lease identity to delete through. */
  lc_lease_ref lease;
  /** Attachment id or name selector. */
  lc_attachment_selector selector;
} lc_attachment_delete_op;

/** Client-level operation used to delete all attachments through an existing,
 * credentialed lease reference. */
typedef struct lc_attachment_delete_all_op {
  /** Existing lease identity whose attachments should be deleted. */
  lc_lease_ref lease;
} lc_attachment_delete_all_op;

/** Result metadata returned by attachment fetch operations. */
typedef struct lc_attachment_get_res {
  /** Metadata for the fetched attachment. */
  lc_attachment_info attachment;
  /** Server correlation id for the fetch response. */
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
   * Parses the current state document into `dst` through a lonejson map.
   *
   * Callers define their struct and map with `LONEJSON_FIELD_*` and
   * `LONEJSON_MAP_DEFINE(...)`, then release lonejson-owned field storage with
   * a lonejson runtime cleanup call when finished. For very large string or
   * byte values, use lonejson spool-backed field mappings so load can spill
   * those fields instead of forcing them to stay resident in memory.
   */
  int (*load)(lc_lease *self, const lonejson_map *map, void *dst,
              const lc_get_opts *opts, lc_get_res *out, lc_error *error);
  /**
   * Convenience mapped-struct variant of `update()`.
   *
   * The caller supplies the same lonejson map and struct shape used for
   * `load()`. lonejson serializes the mapped struct to JSON, and liblockdc
   * streams that JSON into the bound lease update. For very large outbound text
   * or byte values, use lonejson source-backed fields so serialization can read
   * from files or file descriptors without first materializing those bytes in
   * memory.
   */
  int (*save)(lc_lease *self, const lonejson_map *map, const void *src,
              lc_error *error);
  /**
   * Replaces the state document from a streamed JSON source.
   *
   * Use this for large JSON payloads. `opts` may be `NULL` for default update
   * behavior. On success, `self->version` and `self->state_etag` are refreshed.
   */
  int (*update)(lc_lease *self, lc_source *src, const lc_update_opts *opts,
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
  lc_version version;
  /** Current lease expiry as a Unix timestamp. */
  lc_unix_seconds lease_expires_at_unix;
  /** Current private state etag. */
  const char *state_etag;
  /** Non-zero when `query_hidden` was explicitly set by the server. */
  int has_query_hidden;
  /** Whether the state is hidden from normal query results. */
  int query_hidden;

  /** Private implementation pointer; callers must not inspect or modify it. */
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
   * Acknowledges the message and closes a non-batch handle on success.
   *
   * Batch messages remain owned by their `lc_dequeue_batch_res` and are closed
   * by `lc_dequeue_batch_cleanup()`.
   *
   * If this call fails, the handle remains valid and may be retried or closed.
   */
  int (*ack)(lc_message *self, lc_error *error);
  /**
   * Negatively acknowledges the message and closes a non-batch handle on
   * success. Batch messages remain owned by their `lc_dequeue_batch_res` and
   * are closed by `lc_dequeue_batch_cleanup()`.
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
  /**
   * Copies the payload stream into `dst`.
   *
   * Resettable payloads are rewound before copying. Single-pass payloads are
   * copied from their current position.
   */
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
  lc_unix_seconds not_visible_until_unix;
  /** Current visibility timeout in seconds. */
  long visibility_timeout_seconds;
  /** Payload content type recorded with the message. */
  const char *payload_content_type;
  /** Server correlation identifier for the dequeue response. */
  const char *correlation_id;
  /** Delivery lease identifier for the message. */
  const char *lease_id;
  /** Current delivery lease expiry as a Unix timestamp. */
  lc_unix_seconds lease_expires_at_unix;
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

  /** Private implementation pointer; callers must not inspect or modify it. */
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

  /** Private implementation pointer; callers must not inspect or modify it. */
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
  /**
   * Convenience variant of `get()` that parses the current state into `dst`
   * through a lonejson map.
   *
   * Callers define their struct and map with `LONEJSON_FIELD_*` and
   * `LONEJSON_MAP_DEFINE(...)`, then release lonejson-owned field storage with
   * a lonejson runtime cleanup call when finished.
   */
  int (*load)(lc_client *self, const char *key, const lonejson_map *map,
              void *dst, const lc_get_opts *opts, lc_get_res *out,
              lc_error *error);
  /** Updates an existing, credentialed lease reference from a streamed JSON
   * source. */
  int (*update)(lc_client *self, const lc_update_req *req, lc_source *src,
                lc_update_res *out, lc_error *error);
  /** Applies one or more server-side mutations to an existing, credentialed
   * lease reference. */
  int (*mutate)(lc_client *self, const lc_mutate_op *req, lc_mutate_res *out,
                lc_error *error);
  /** Updates metadata fields on an existing, credentialed lease reference. */
  int (*metadata)(lc_client *self, const lc_metadata_op *req,
                  lc_metadata_res *out, lc_error *error);
  /** Removes state bytes for an existing, credentialed lease reference. */
  int (*remove)(lc_client *self, const lc_remove_op *req, lc_remove_res *out,
                lc_error *error);
  /** Renews an existing lease reference without using a bound `lc_lease`. */
  int (*keepalive)(lc_client *self, const lc_keepalive_op *req,
                   lc_keepalive_res *out, lc_error *error);
  /** Releases an existing lease reference without using a bound `lc_lease`. */
  int (*release)(lc_client *self, const lc_release_op *req, lc_release_res *out,
                 lc_error *error);
  /** Streams an attachment upload for an existing, credentialed lease
   * reference. */
  int (*attach)(lc_client *self, const lc_attach_op *req, lc_source *src,
                lc_attach_res *out, lc_error *error);
  /** Lists the attachments associated with an existing lease reference. */
  int (*list_attachments)(lc_client *self, const lc_attachment_list_req *req,
                          lc_attachment_list *out, lc_error *error);
  /** Streams an attachment download for an existing lease reference. */
  int (*get_attachment)(lc_client *self, const lc_attachment_get_op *req,
                        lc_sink *dst, lc_attachment_get_res *out,
                        lc_error *error);
  /** Deletes one attachment through an existing, credentialed lease
   * reference. */
  int (*delete_attachment)(lc_client *self, const lc_attachment_delete_op *req,
                           int *deleted, lc_error *error);
  /** Deletes all attachments through an existing, credentialed lease
   * reference. */
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
  /** Reapplies a durable transaction decision by identifier. An expired
   * prepared decision is converted to rollback before it is applied. */
  int (*txn_replay)(lc_client *self, const lc_txn_replay_req *req,
                    lc_txn_replay_res *out, lc_error *error);
  /** Prepares a transaction decision. */
  int (*txn_prepare)(lc_client *self, const lc_txn_decision_req *req,
                     lc_txn_decision_res *out, lc_error *error);
  /** Commits and applies a durable transaction decision. Matching participant
   * leases are released as part of the decision. */
  int (*txn_commit)(lc_client *self, const lc_txn_decision_req *req,
                    lc_txn_decision_res *out, lc_error *error);
  /** Rolls back and applies a durable transaction decision. Matching
   * participant leases are released as part of the decision. */
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
  /**
   * Dequeues a queue message without an associated state lease.
   * On error, no delivery lease is retained by the client.
   */
  int (*dequeue)(lc_client *self, const lc_dequeue_req *req, lc_message **out,
                 lc_error *error);
  /**
   * Dequeues up to `req->page_size` messages and returns them as a batch.
   * On error, no delivery lease is retained by the client.
   */
  int (*dequeue_batch)(lc_client *self, const lc_dequeue_req *req,
                       lc_dequeue_batch_res *out, lc_error *error);
  /**
   * Dequeues a queue message and its associated state lease handle atomically.
   * On error, neither lease is retained by the client.
   */
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
   * outlive the root client. Remote workers use cloned client instances. Pouch
   * workers retain and share the source client's local session so the default
   * exclusive writer remains a single root owner; closing the source client
   * after service creation does not release that session until the service is
   * closed.
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
  /** Private implementation pointer; callers must not inspect or modify it. */
  void *impl;
  /**
   * Acquires a lease, fetches the private state snapshot, invokes `handler`,
   * closes the snapshot, and releases the lease before returning.
   *
   * The `lc_acquire_for_update_context`, its `lease`, and its `state.reader`
   * are borrowed and valid only for the duration of the handler call. The
   * helper owns the final release, so handlers must not close or release the
   * borrowed lease. Handler success commits staged changes; handler failure
   * releases with rollback.
   *
   * Extension slots are appended to keep method offsets stable within the
   * current shared-library ABI line.
   */
  int (*acquire_for_update)(lc_client *self, const lc_acquire_req *req,
                            lc_acquire_for_update_handler_fn handler,
                            void *handler_context, lc_error *error);
  /**
   * Streams key query result strings through chunked callbacks.
   *
   * Extension slots are appended to keep method offsets stable within the
   * current shared-library ABI line.
   */
  int (*query_keys)(lc_client *self, const lc_query_req *req,
                    const lc_query_key_handler *handler, void *context,
                    lc_query_res *out, lc_error *error);
};

/**
 * Returns the semantic version string compiled into this build.
 *
 * The returned process-lifetime string is borrowed and must not be freed.
 */
const char *lc_version_string(void);

/**
 * Resets an `lc_error` to a known empty state.
 *
 * Accepts `NULL`. Cleanup an error that already owns fields before
 * reinitializing it.
 */
void lc_error_init(lc_error *error);
/**
 * Releases all heap-owned fields inside an `lc_error` and zeroes it.
 *
 * Accepts `NULL` and an already-zeroed error.
 */
void lc_error_cleanup(lc_error *error);
/**
 * Initializes an allocator override to use the default allocator.
 *
 * Accepts `NULL`. The resulting all-zero hook set selects libc allocation.
 */
void lc_allocator_init(lc_allocator *allocator);
/**
 * Initializes client configuration with public defaults.
 *
 * Accepts `NULL`. The initialized config has a 30-second timeout, prefers
 * HTTP/2, uses the default 100 MiB typed-JSON limit, enables normal mTLS peer
 * verification, and leaves endpoint, namespace, logger, allocator, and Pouch
 * options unset.
 */
void lc_client_config_init(lc_client_config *config);

/**
 * @name Request and callback initializers
 *
 * Every initializer in this group accepts `NULL` and otherwise resets the
 * entire object. Request initializers produce the documented zero/default
 * behavior; they do not release fields previously owned by the object.
 * @{ */
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
/**
 * Initializes one managed consumer config with one worker and the default
 * restart policy; all other fields are zero/empty.
 */
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
/** @} */

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

/**
 * Creates a rewindable source backed by caller-provided memory.
 *
 * The bytes are borrowed; they must remain valid and unchanged until the
 * source is closed. The caller owns the returned source.
 */
int lc_source_from_memory(const void *bytes, size_t length, lc_source **out,
                          lc_error *error);
/**
 * Opens a rewindable source backed by a file path.
 *
 * The returned source owns its file descriptor and closes it from
 * `lc_source_close()`. The caller owns the returned source.
 */
int lc_source_from_file(const char *path, lc_source **out, lc_error *error);
/**
 * Wraps a borrowed file descriptor as a single-pass source.
 *
 * Closing the source does not close `fd`; the caller retains descriptor
 * ownership. The source reads from and advances the descriptor's current file
 * offset. Pass a duplicate when the source needs an independent offset or
 * lifetime.
 */
int lc_source_from_fd(int fd, lc_source **out, lc_error *error);
/**
 * Wraps caller-provided callbacks as a source.
 *
 * `read` is required. `reset` may be `NULL` for single-pass streams, in which
 * case APIs that require a rewindable source return `LC_ERR_INVALID`. `close`
 * may be `NULL`; when present it is called from `lc_source_close()`. The
 * caller owns the returned source.
 */
int lc_source_from_callbacks(lc_source_read_fn read, lc_source_reset_fn reset,
                             lc_source_close_fn close, void *context,
                             lc_source **out, lc_error *error);
/**
 * Creates a sink that truncates and writes a file path.
 *
 * The sink owns the opened descriptor and closes it with `lc_sink_close()`.
 */
int lc_sink_to_file(const char *path, lc_sink **out, lc_error *error);
/**
 * Creates a sink that writes bytes to a borrowed file descriptor.
 *
 * Closing the sink does not close `fd`; the caller retains descriptor
 * ownership. Pass a duplicate when the sink needs an independent lifetime.
 */
int lc_sink_to_fd(int fd, lc_sink **out, lc_error *error);
/** Creates a caller-owned sink that accepts and discards all bytes. */
int lc_sink_to_discard(lc_sink **out, lc_error *error);
/** Returns non-zero when a sink was created by `lc_sink_to_discard()`. */
int lc_sink_is_discard(const lc_sink *sink);
/**
 * Creates a caller-owned in-memory sink.
 *
 * The sink grows as bytes are written and releases its accumulated buffer when
 * closed.
 */
int lc_sink_to_memory(lc_sink **out, lc_error *error);
/**
 * Returns the bytes accumulated by an in-memory sink.
 *
 * The returned buffer is borrowed from `sink` and remains valid until the sink
 * is written again or closed.
 */
int lc_sink_memory_bytes(lc_sink *sink, const void **bytes, size_t *length,
                         lc_error *error);
/**
 * Copies all bytes from a source into a sink.
 *
 * The copy is streaming and uses a bounded internal buffer. It resets `error`
 * for this operation before reading the source. It advances `src`, does not
 * reset it, and leaves both handles open. `written` may be `NULL`; on success
 * it receives the total bytes copied.
 */
int lc_copy(lc_source *src, lc_sink *dst, size_t *written, lc_error *error);

/** Generates a new `lc-pouch-key-v1:<base64url>` root key string.
 *
 * On success `out` receives a liblockdc-owned allocation transferred to the
 * caller. Release it with `lc_pouch_crypto_key_string_free()`.
 */
int lc_pouch_crypto_generate_key_string(char **out, lc_error *error);

/**
 * Releases a key string or path returned by Pouch crypto helpers.
 *
 * Accepts `NULL`. Do not use this helper for caller-owned key strings.
 */
void lc_pouch_crypto_key_string_free(char *key_string);

/** Returns the default Pouch crypto key-file path.
 *
 * The path is `$XDG_CONFIG_HOME/liblockdc/pouch.key`, or
 * `$HOME/.config/liblockdc/pouch.key` when `XDG_CONFIG_HOME` is unset. The
 * returned string must be released with
 * `lc_pouch_crypto_key_string_free()`.
 */
int lc_pouch_crypto_default_key_file(char **out, lc_error *error);

/** Generates and writes a Pouch root key file with mode `0600`.
 *
 * Parent directories are created with mode `0700`. When `overwrite` is zero,
 * an existing key file is rejected. When `key_string_out` is non-NULL, the
 * generated key string is returned and must be released with
 * `lc_pouch_crypto_key_string_free()`.
 */
int lc_pouch_crypto_generate_key_file(const char *path, int overwrite,
                                      char **key_string_out, lc_error *error);

/** Closes and frees a client handle. Accepts `NULL`. */
void lc_client_close(lc_client *client);
/** Closes and frees a lease handle. Accepts `NULL`. */
void lc_lease_close(lc_lease *lease);
/** Closes and frees a message handle. Accepts `NULL`. */
void lc_message_close(lc_message *message);
/** Closes and frees a source. Accepts `NULL`. */
void lc_source_close(lc_source *source);
/** Closes and frees a sink. Accepts `NULL`. */
void lc_sink_close(lc_sink *sink);

/**
 * @name Owned response cleanup
 *
 * These helpers release all nested strings, arrays, handles, and metadata
 * owned by their response, then zero the object. Every helper accepts `NULL`
 * and an already-zeroed object.
 * @{ */
/** Releases fields owned by an `lc_describe_res`. */
void lc_describe_res_cleanup(lc_describe_res *response);
/** Releases fields owned by an `lc_get_res`. */
void lc_get_res_cleanup(lc_get_res *response);
/** Releases fields owned by an `lc_update_res`. */
void lc_update_res_cleanup(lc_update_res *response);
/** Releases fields owned by an `lc_mutate_res`. */
void lc_mutate_res_cleanup(lc_mutate_res *response);
/** Releases fields owned by an `lc_metadata_res`. */
void lc_metadata_res_cleanup(lc_metadata_res *response);
/** Releases fields owned by an `lc_remove_res`. */
void lc_remove_res_cleanup(lc_remove_res *response);
/** Releases fields owned by an `lc_keepalive_res`. */
void lc_keepalive_res_cleanup(lc_keepalive_res *response);
/** Releases fields owned by an `lc_release_res`. */
void lc_release_res_cleanup(lc_release_res *response);
/** Releases fields owned by an `lc_query_res`. */
void lc_query_res_cleanup(lc_query_res *response);
/** Releases every string and the array owned by an `lc_string_list`. */
void lc_string_list_cleanup(lc_string_list *response);
/** Releases fields owned by an `lc_namespace_config_res`. */
void lc_namespace_config_res_cleanup(lc_namespace_config_res *response);
/** Releases fields owned by an `lc_index_flush_res`. */
void lc_index_flush_res_cleanup(lc_index_flush_res *response);
/** Releases fields owned by an `lc_txn_replay_res`. */
void lc_txn_replay_res_cleanup(lc_txn_replay_res *response);
/** Releases fields owned by an `lc_txn_decision_res`. */
void lc_txn_decision_res_cleanup(lc_txn_decision_res *response);
/** Releases fields owned by an `lc_tc_lease_acquire_res`. */
void lc_tc_lease_acquire_res_cleanup(lc_tc_lease_acquire_res *response);
/** Releases fields owned by an `lc_tc_lease_renew_res`. */
void lc_tc_lease_renew_res_cleanup(lc_tc_lease_renew_res *response);
/** Releases fields owned by an `lc_tc_lease_release_res`. */
void lc_tc_lease_release_res_cleanup(lc_tc_lease_release_res *response);
/** Releases fields owned by an `lc_tc_leader_res`. */
void lc_tc_leader_res_cleanup(lc_tc_leader_res *response);
/** Releases fields owned by an `lc_tc_cluster_res`. */
void lc_tc_cluster_res_cleanup(lc_tc_cluster_res *response);
/** Releases fields owned by an `lc_tc_rm_res`. */
void lc_tc_rm_res_cleanup(lc_tc_rm_res *response);
/** Releases every backend entry owned by an `lc_tc_rm_list_res`. */
void lc_tc_rm_list_res_cleanup(lc_tc_rm_list_res *response);
/** Releases fields owned by an `lc_enqueue_res`. */
void lc_enqueue_res_cleanup(lc_enqueue_res *response);
/** Releases fields owned by an `lc_queue_stats_res`. */
void lc_queue_stats_res_cleanup(lc_queue_stats_res *response);
/** Releases fields owned by an `lc_ack_res`. */
void lc_ack_res_cleanup(lc_ack_res *response);
/** Releases fields owned by an `lc_nack_res`. */
void lc_nack_res_cleanup(lc_nack_res *response);
/** Releases fields owned by an `lc_extend_res`. */
void lc_extend_res_cleanup(lc_extend_res *response);
/** Closes every message still owned by a dequeue batch and clears it. */
void lc_dequeue_batch_cleanup(lc_dequeue_batch_res *response);
/** Releases fields owned by an `lc_watch_event`. */
void lc_watch_event_cleanup(lc_watch_event *event);
/** Releases fields owned by an `lc_attachment_info`. */
void lc_attachment_info_cleanup(lc_attachment_info *info);
/** Releases fields owned by an `lc_attach_res`. */
void lc_attach_res_cleanup(lc_attach_res *response);
/** Releases every attachment owned by an `lc_attachment_list`. */
void lc_attachment_list_cleanup(lc_attachment_list *response);
/** Releases fields owned by an `lc_attachment_get_res`. */
void lc_attachment_get_res_cleanup(lc_attachment_get_res *response);
/** @} */

/** Acquires a lease and returns a bound `lc_lease` handle for follow-up work.
 */
int lc_acquire(lc_client *client, const lc_acquire_req *req, lc_lease **out,
               lc_error *error);
/**
 * Acquires a lease, fetches the private state snapshot, runs `handler`, and
 * always attempts to release the lease before returning. The handler receives a
 * borrowed lease and must not close or release it. Handler success commits
 * staged changes; handler failure releases with rollback.
 */
int lc_acquire_for_update(lc_client *client, const lc_acquire_req *req,
                          lc_acquire_for_update_handler_fn handler,
                          void *handler_context, lc_error *error);
/** Describes the current lease and state metadata for a key. */
int lc_describe(lc_client *client, const lc_describe_req *req,
                lc_describe_res *out, lc_error *error);
/** Streams a state document into `dst`. Prefer the lease method once acquired.
 */
int lc_get(lc_client *client, const char *key, const lc_get_opts *opts,
           lc_sink *dst, lc_get_res *out, lc_error *error);
/**
 * Parses state into a caller-provided struct through a lonejson map.
 *
 * Ordinary mapped fields are materialized. Spool-backed lonejson fields may
 * remain file-backed; use `lc_get()` with a sink for fully streamed transfer.
 */
int lc_load(lc_client *client, const char *key, const lonejson_map *map,
            void *dst, const lc_get_opts *opts, lc_get_res *out,
            lc_error *error);
/** Updates an existing, credentialed lease reference from a streamed source. */
int lc_update(lc_client *client, const lc_update_req *req, lc_source *src,
              lc_update_res *out, lc_error *error);
/** Applies one or more server-side mutations to a credentialed lease
 * reference. */
int lc_mutate(lc_client *client, const lc_mutate_op *req, lc_mutate_res *out,
              lc_error *error);
/** Updates metadata fields on a credentialed lease reference. */
int lc_metadata(lc_client *client, const lc_metadata_op *req,
                lc_metadata_res *out, lc_error *error);
/** Removes state bytes for a credentialed lease reference while keeping the
 * lease alive. */
int lc_remove(lc_client *client, const lc_remove_op *req, lc_remove_res *out,
              lc_error *error);
/** Renews an existing lease reference without a bound `lc_lease` handle. */
int lc_keepalive(lc_client *client, const lc_keepalive_op *req,
                 lc_keepalive_res *out, lc_error *error);
/** Releases an existing lease reference without a bound `lc_lease` handle. */
int lc_release(lc_client *client, const lc_release_op *req, lc_release_res *out,
               lc_error *error);
/** Streams an attachment upload for a credentialed lease reference. */
int lc_attach(lc_client *client, const lc_attach_op *req, lc_source *src,
              lc_attach_res *out, lc_error *error);
/** Lists attachments associated with a lease reference. */
int lc_list_attachments(lc_client *client, const lc_attachment_list_req *req,
                        lc_attachment_list *out, lc_error *error);
/** Streams an attachment download into `dst`. */
int lc_get_attachment(lc_client *client, const lc_attachment_get_op *req,
                      lc_sink *dst, lc_attachment_get_res *out,
                      lc_error *error);
/** Deletes one attachment through a credentialed lease reference. */
int lc_delete_attachment(lc_client *client, const lc_attachment_delete_op *req,
                         int *deleted, lc_error *error);
/** Deletes all attachments through a credentialed lease reference. */
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
/** Streams key query result strings through chunked callbacks. */
int lc_query_keys(lc_client *client, const lc_query_req *req,
                  const lc_query_key_handler *handler, void *context,
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
/** Reapplies a durable transaction decision by identifier. An expired prepared
 * decision is converted to rollback before it is applied. */
int lc_txn_replay(lc_client *client, const lc_txn_replay_req *req,
                  lc_txn_replay_res *out, lc_error *error);
/** Prepares a transaction decision. */
int lc_txn_prepare(lc_client *client, const lc_txn_decision_req *req,
                   lc_txn_decision_res *out, lc_error *error);
/** Commits and applies a durable transaction decision. Matching participant
 * leases are released as part of the decision. */
int lc_txn_commit(lc_client *client, const lc_txn_decision_req *req,
                  lc_txn_decision_res *out, lc_error *error);
/** Rolls back and applies a durable transaction decision. Matching participant
 * leases are released as part of the decision. */
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
/**
 * Dequeues a message and returns an `lc_message` handle for follow-up calls.
 * On error, no delivery lease is retained by the client.
 */
int lc_dequeue(lc_client *client, const lc_dequeue_req *req, lc_message **out,
               lc_error *error);
/**
 * Dequeues up to `req->page_size` messages and returns them as a batch.
 * On error, no delivery lease is retained by the client.
 */
int lc_dequeue_batch(lc_client *client, const lc_dequeue_req *req,
                     lc_dequeue_batch_res *out, lc_error *error);
/**
 * Dequeues a message and its associated state lease handle atomically.
 * On error, neither lease is retained by the client.
 */
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
/**
 * Parses bound-lease state into a caller-provided struct through lonejson.
 *
 * Ordinary mapped fields are materialized. Spool-backed lonejson fields may
 * remain file-backed; use `lc_lease_get()` for fully streamed transfer.
 */
int lc_lease_load(lc_lease *lease, const lonejson_map *map, void *dst,
                  const lc_get_opts *opts, lc_get_res *out, lc_error *error);
/** Convenience mapped-struct `update()` variant for lonejson-compatible data.
 */
int lc_lease_save(lc_lease *lease, const lonejson_map *map, const void *src,
                  lc_error *error);
/** Streams a replacement state document into a bound lease. */
int lc_lease_update(lc_lease *lease, lc_source *src, const lc_update_opts *opts,
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
/**
 * Returns the payload reader owned by the message handle.
 *
 * The borrowed reader may be single-pass for live subscription deliveries;
 * do not close it independently of the message.
 */
lc_source *lc_message_payload(lc_message *message);
/** Rewinds a bound message payload so it can be consumed again. */
int lc_message_rewind_payload(lc_message *message, lc_error *error);
/**
 * Copies a bound message payload into `dst`.
 *
 * Resettable payloads are rewound before copying. Single-pass payloads are
 * copied from their current position.
 */
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
