#include "lc_api_internal.h"

#include <string.h>

void lc_collection_config_init(lc_collection_config *config) {
  if (config != NULL)
    memset(config, 0, sizeof(*config));
}
void lc_collection_append_req_init(lc_collection_append_req *req) {
  if (req != NULL)
    memset(req, 0, sizeof(*req));
}
void lc_collection_metadata_req_init(lc_collection_metadata_req *req) {
  if (req != NULL)
    memset(req, 0, sizeof(*req));
}
void lc_collection_read_opts_init(lc_collection_read_opts *opts) {
  if (opts != NULL)
    memset(opts, 0, sizeof(*opts));
}
void lc_collection_read_res_init(lc_collection_read_res *result) {
  if (result != NULL)
    memset(result, 0, sizeof(*result));
}
void lc_collection_read_res_cleanup(lc_collection_read_res *result) {
  if (result == NULL)
    return;
  lc_free_with_allocator(NULL, result->resume_cursor);
  lc_free_with_allocator(NULL, result->checkpoint_cursor);
  memset(result, 0, sizeof(*result));
}

static int lc_collection_bad_receiver(lc_error *error) {
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "operation requires a supporting collection receiver",
                      NULL, NULL, NULL);
}

int lc_client_create_collection(lc_client *client,
                                const lc_collection_config *config,
                                lc_collection **out, lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (client == NULL || client->create_collection == NULL)
    return lc_collection_bad_receiver(error);
  return client->create_collection(client, config, out, error);
}
int lc_client_open_collection(lc_client *client,
                              const lc_collection_config *config,
                              lc_collection **out, lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (client == NULL || client->open_collection == NULL)
    return lc_collection_bad_receiver(error);
  return client->open_collection(client, config, out, error);
}
int lc_client_list_collections(lc_client *client, const char *ns,
                               lc_collection_visit_fn visitor, void *context,
                               lc_error *error) {
  if (client == NULL || client->list_collections == NULL)
    return lc_collection_bad_receiver(error);
  return client->list_collections(client, ns, visitor, context, error);
}
int lc_client_collection_capabilities(lc_client *client,
                                      lc_collection_capabilities *out,
                                      lc_error *error) {
  if (client == NULL || client->collection_capabilities == NULL)
    return lc_collection_bad_receiver(error);
  return client->collection_capabilities(client, out, error);
}
int lc_collection_describe(lc_collection *collection, lc_collection_info *out,
                           lc_error *error) {
  if (collection == NULL || collection->describe == NULL)
    return lc_collection_bad_receiver(error);
  return collection->describe(collection, out, error);
}
int lc_collection_metadata(lc_collection *collection,
                           const lc_collection_metadata_req *req,
                           lc_collection_info *out, lc_error *error) {
  if (collection == NULL || collection->metadata == NULL)
    return lc_collection_bad_receiver(error);
  return collection->metadata(collection, req, out, error);
}
int lc_collection_entry_metadata(lc_collection *collection, uint64_t position,
                                 const lc_collection_metadata_req *req,
                                 lc_collection_append_res *out,
                                 lc_error *error) {
  if (collection == NULL || collection->entry_metadata == NULL)
    return lc_collection_bad_receiver(error);
  return collection->entry_metadata(collection, position, req, out, error);
}
int lc_collection_append(lc_collection *collection,
                         const lc_collection_append_req *req, lc_source *source,
                         lc_collection_append_res *out, lc_error *error) {
  if (collection == NULL || collection->append == NULL)
    return lc_collection_bad_receiver(error);
  return collection->append(collection, req, source, out, error);
}
int lc_collection_append_status(lc_collection *collection,
                                const char *append_id,
                                lc_collection_append_status_res *out,
                                lc_error *error) {
  if (collection == NULL || collection->append_status == NULL)
    return lc_collection_bad_receiver(error);
  return collection->append_status(collection, append_id, out, error);
}
int lc_collection_read(lc_collection *collection,
                       const lc_collection_read_opts *opts, lc_sink *sink,
                       lc_collection_read_res *out, lc_error *error) {
  if (collection == NULL || collection->read == NULL)
    return lc_collection_bad_receiver(error);
  return collection->read(collection, opts, sink, out, error);
}
int lc_collection_delete_entry(lc_collection *collection, uint64_t position,
                               lc_collection_delete_res *out, lc_error *error) {
  if (collection == NULL || collection->delete_entry == NULL)
    return lc_collection_bad_receiver(error);
  return collection->delete_entry(collection, position, out, error);
}
int lc_collection_drop(lc_collection *collection, lc_error *error) {
  if (collection == NULL || collection->drop == NULL)
    return lc_collection_bad_receiver(error);
  return collection->drop(collection, error);
}
void lc_collection_close(lc_collection *collection) {
  if (collection != NULL && collection->close != NULL)
    collection->close(collection);
}
