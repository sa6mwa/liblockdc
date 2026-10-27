/* Usage: collection_journal pouch:///absolute/path
 * Creates a uniquely named example collection, streams its raw JSON to stdout,
 * exercises retry/deletion/retention, and drops that collection before exit.
 * Uses only the installed public SDK. */
#include <lc/lc.h>

#include <stdio.h>
#include <string.h>

static int output(lc_sink *sink, const void *bytes, size_t length,
                  lc_error *error) {
  (void)sink;
  if (fwrite(bytes, 1U, length, stdout) != length) {
    (void)lc_error_set(error, LC_ERR_TRANSPORT, 0, "stdout write failed", NULL,
                       NULL, NULL);
    return 0;
  }
  return 1;
}

static int boundary(void *context, const lc_collection_read_metadata *metadata,
                    lc_error *error) {
  uint64_t *last = (uint64_t *)context;
  if (metadata->kind == LC_COLLECTION_READ_METADATA_ENTRY) {
    if (metadata->position <= *last || metadata->resume_cursor == NULL)
      return lc_error_set(error, LC_ERR_PROTOCOL, 0,
                          "invalid completed entry metadata", NULL, NULL, NULL);
    *last = metadata->position;
  }
  return LC_OK;
}

static int append(lc_collection *collection, const char *id, const char *json,
                  uint64_t position, int duplicate, lc_error *error) {
  lc_collection_append_req request;
  lc_collection_append_res result;
  lc_source *source;
  int rc;
  source = NULL;
  lc_collection_append_req_init(&request);
  request.append_id = id;
  request.has_retention_seconds = 1;
  request.retention_seconds = 7L * 24L * 60L * 60L;
  rc = lc_source_from_memory(json, strlen(json), &source, error);
  if (rc == LC_OK)
    rc = collection->append(collection, &request, source, &result, error);
  lc_source_close(source);
  if (rc == LC_OK && (result.position != position ||
                      result.duplicate != duplicate || result.deleted))
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0, "unexpected append outcome",
                      NULL, NULL, NULL);
  return rc;
}

int main(int argc, char **argv) {
  const char *endpoints[1];
  char name[LC_XID_STRING_SIZE];
  lc_client_config config;
  lc_collection_config cc;
  lc_collection_capabilities capabilities;
  lc_collection_metadata_req metadata;
  lc_collection_append_res updated;
  lc_collection_delete_res deleted;
  lc_collection_append_status_res status;
  lc_collection_read_opts options;
  lc_collection_read_res result;
  lc_collection_info info;
  lc_collection *collection;
  lc_client *client;
  lc_sink sink;
  lc_error error;
  uint64_t last;
  char checkpoint[4097];
  int rc;
  if (argc != 2) {
    fprintf(stderr, "usage: %s pouch:///absolute/path\n", argv[0]);
    return 1;
  }
  client = NULL;
  collection = NULL;
  last = 0;
  lc_error_init(&error);
  lc_client_config_init(&config);
  lc_collection_config_init(&cc);
  lc_collection_metadata_req_init(&metadata);
  lc_collection_read_opts_init(&options);
  lc_collection_read_res_init(&result);
  endpoints[0] = argv[1];
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "examples";
  rc = lc_xid_new(name, &error);
  if (rc == LC_OK)
    rc = lc_client_open(&config, &client, &error);
  if (rc == LC_OK)
    rc = client->collection_capabilities(client, &capabilities, &error);
  if (rc == LC_OK && (!capabilities.collections || !capabilities.ranges ||
                      !capabilities.first_n))
    rc = lc_error_set(&error, LC_ERR_UNSUPPORTED, 0,
                      "endpoint does not support bounded collection reads",
                      NULL, NULL, NULL);
  cc.name = name;
  cc.has_retention_seconds = 1;
  cc.retention_seconds = 30L * 24L * 60L * 60L;
  if (rc == LC_OK)
    rc = client->create_collection(client, &cc, &collection, &error);
  if (rc == LC_OK)
    rc = append(collection, "first", "{ \"event\" : \"created\" }", 1U, 0,
                &error);
  if (rc == LC_OK)
    rc = append(collection, "second", "{\"event\":\"completed\"}", 2U, 0,
                &error);
  if (rc == LC_OK)
    rc = append(collection, "first", "{\"event\":\"created\"}", 1U, 1, &error);
  /* Metadata clears entry expiry without touching its immutable JSON body. */
  metadata.has_retention_seconds = 1;
  metadata.retention_seconds = 0;
  if (rc == LC_OK)
    rc =
        collection->entry_metadata(collection, 1U, &metadata, &updated, &error);
  if (rc == LC_OK && (!updated.has_retention_expires_at_unix ||
                      updated.retention_expires_at_unix != 0))
    rc = LC_ERR_PROTOCOL;
  memset(&sink, 0, sizeof(sink));
  sink.write = output;
  options.on_metadata = boundary;
  options.metadata_context = &last;
  options.has_after = options.has_through = options.has_first_n = 1;
  options.after = 0U;
  options.through = 2U;
  options.first_n = 1U;
  if (rc == LC_OK)
    rc = collection->read(collection, &options, &sink, &result, &error);
  if (rc == LC_OK && (!result.complete || result.completed_records != 1U ||
                      result.checkpoint_cursor == NULL || last != 1U))
    rc = LC_ERR_PROTOCOL;
  if (rc == LC_OK && strlen(result.checkpoint_cursor) >= sizeof(checkpoint))
    rc = LC_ERR_PROTOCOL;
  if (rc == LC_OK) {
    strcpy(checkpoint, result.checkpoint_cursor);
    lc_collection_read_res_cleanup(&result);
    options.cursor = checkpoint;
    rc = collection->read(collection, &options, &sink, &result, &error);
  }
  if (rc == LC_OK && (!result.complete || result.completed_records != 1U ||
                      result.checkpoint_cursor == NULL || last != 2U))
    rc = LC_ERR_PROTOCOL;
  if (rc == LC_OK && fflush(stdout) != 0)
    rc = LC_ERR_TRANSPORT;
  /* Persist result.checkpoint_cursor after durably accepting output to consume
   * later appends. This example instead demonstrates permanent deletion. */
  if (rc == LC_OK)
    rc = collection->delete_entry(collection, 1U, &deleted, &error);
  if (rc == LC_OK)
    rc = collection->append_status(collection, "first", &status, &error);
  if (rc == LC_OK && status.state != LC_COLLECTION_APPEND_DELETED)
    rc = LC_ERR_PROTOCOL;
  if (rc == LC_OK) {
    collection->close(collection);
    collection = NULL;
    cc.has_retention_seconds = 0; /* Open never changes retention. */
    rc = client->open_collection(client, &cc, &collection, &error);
  }
  if (rc == LC_OK)
    rc = collection->describe(collection, &info, &error);
  if (rc == LC_OK && (info.live_count != 1U || info.upper_position != 2U))
    rc = LC_ERR_PROTOCOL;
  if (rc == LC_OK)
    rc = collection->drop(collection, &error);
  if (rc != LC_OK) {
    fprintf(stderr, "collection example failed (%d): %s\n", rc,
            error.message == NULL ? "" : error.message);
    if (collection != NULL)
      (void)collection->drop(collection, NULL);
  }
  lc_collection_read_res_cleanup(&result);
  lc_collection_close(collection);
  lc_client_close(client);
  lc_error_cleanup(&error);
  return rc == LC_OK ? 0 : 1;
}
