#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define LC_POUCH_HISTORY_MAGIC "LCH1"

struct lc_history_consumer_handle {
  lc_history_consumer pub;
  lc_client_handle *client;
  char *ns;
  char *consumer_id;
  int registered;
};

typedef enum lc_pouch_history_operation {
  LC_POUCH_HISTORY_REGISTER = 1,
  LC_POUCH_HISTORY_POSITION = 2,
  LC_POUCH_HISTORY_ADVANCE = 3,
  LC_POUCH_HISTORY_UNREGISTER = 4
} lc_pouch_history_operation;

typedef struct lc_pouch_history_operation_context {
  lc_history_consumer_handle *consumer;
  lc_pouch_history_operation operation;
  lc_index_seq requested_acknowledged_index_seq;
  lc_history_consumer_position *out;
} lc_pouch_history_operation_context;

static int lc_pouch_history_directory(lc_pouch *pouch, const char *ns,
                                      int create, char **out, lc_error *error) {
  char *control_directory;
  char *history_directory;
  char *namespace_leaf;
  char *namespace_directory;
  int rc;

  *out = NULL;
  control_directory =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, ".lockd");
  history_directory =
      control_directory != NULL
          ? lc_pouch_path_join(&pouch->allocator, control_directory,
                               "history-consumers")
          : NULL;
  namespace_leaf = lc_pouch_path_escape_name(&pouch->allocator, ns);
  namespace_directory =
      history_directory != NULL && namespace_leaf != NULL
          ? lc_pouch_path_join(&pouch->allocator, history_directory,
                               namespace_leaf)
          : NULL;
  lc_free_with_allocator(&pouch->allocator, namespace_leaf);
  if (control_directory == NULL || history_directory == NULL ||
      namespace_directory == NULL) {
    lc_free_with_allocator(&pouch->allocator, control_directory);
    lc_free_with_allocator(&pouch->allocator, history_directory);
    lc_free_with_allocator(&pouch->allocator, namespace_directory);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch history consumer path", NULL,
                        NULL, "pouch");
  }
  rc = LC_OK;
  if (create) {
    rc = lc_pouch_path_ensure_directory(
        control_directory, "failed to create pouch control directory", error);
    if (rc == LC_OK) {
      rc = lc_pouch_path_ensure_directory(
          history_directory, "failed to create pouch history directory", error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_path_ensure_directory(
          namespace_directory,
          "failed to create pouch namespace history directory", error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_path_fsync_directory(
          pouch->root_path, "failed to sync pouch root history directory",
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_path_fsync_directory(
          control_directory, "failed to sync pouch control history directory",
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_path_fsync_directory(
          history_directory, "failed to sync pouch history directory", error);
    }
  }
  lc_free_with_allocator(&pouch->allocator, control_directory);
  lc_free_with_allocator(&pouch->allocator, history_directory);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, namespace_directory);
    return rc;
  }
  *out = namespace_directory;
  return LC_OK;
}

static int lc_pouch_history_file_path(lc_pouch *pouch, const char *ns,
                                      const char *consumer_id, int create,
                                      char **directory_out, char **path_out,
                                      lc_error *error) {
  char *consumer_name;
  char *consumer_leaf;
  size_t consumer_name_size;
  int rc;

  *directory_out = NULL;
  *path_out = NULL;
  rc = lc_pouch_history_directory(pouch, ns, create, directory_out, error);
  if (rc != LC_OK) {
    return rc;
  }
  consumer_name_size = strlen("consumer:") + strlen(consumer_id) + 1U;
  consumer_name =
      (char *)lc_alloc_with_allocator(&pouch->allocator, consumer_name_size);
  if (consumer_name == NULL) {
    lc_free_with_allocator(&pouch->allocator, *directory_out);
    *directory_out = NULL;
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch history consumer record",
                        NULL, NULL, "pouch");
  }
  snprintf(consumer_name, consumer_name_size, "consumer:%s", consumer_id);
  consumer_leaf = lc_pouch_path_escape_name(&pouch->allocator, consumer_name);
  lc_free_with_allocator(&pouch->allocator, consumer_name);
  *path_out =
      consumer_leaf != NULL
          ? lc_pouch_path_join(&pouch->allocator, *directory_out, consumer_leaf)
          : NULL;
  lc_free_with_allocator(&pouch->allocator, consumer_leaf);
  if (*path_out == NULL) {
    lc_free_with_allocator(&pouch->allocator, *directory_out);
    *directory_out = NULL;
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch history consumer record",
                        NULL, NULL, "pouch");
  }
  return LC_OK;
}

/* Final consumer records are the retention authority.  Stage a replacement in
 * a sibling directory so an interrupted generic atomic write never leaves a
 * `.tmp.<pid>.<attempt>` file among registered consumer identities. */
static int lc_pouch_history_staging_directory(lc_pouch *pouch,
                                              const char *directory, int create,
                                              char **out, lc_error *error) {
  char *staging_directory;
  int rc;

  *out = NULL;
  staging_directory =
      lc_pouch_path_join(&pouch->allocator, directory, ".staging");
  if (staging_directory == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch history staging directory",
                        NULL, directory, "pouch");
  }
  if (!create) {
    *out = staging_directory;
    return LC_OK;
  }
  rc = lc_pouch_path_ensure_directory(
      staging_directory, "failed to create pouch history staging directory",
      error);
  if (rc == LC_OK) {
    rc = lc_pouch_path_fsync_directory(
        directory, "failed to sync pouch history consumer directory", error);
  }
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, staging_directory);
    return rc;
  }
  *out = staging_directory;
  return LC_OK;
}

static int lc_pouch_history_staging_cleanup(lc_pouch *pouch,
                                            const char *directory,
                                            lc_error *error) {
  char *staging_directory;
  DIR *dir;
  struct dirent *entry;
  int removed;
  int rc;

  staging_directory = NULL;
  rc = lc_pouch_history_staging_directory(pouch, directory, 0,
                                          &staging_directory, error);
  if (rc != LC_OK) {
    return rc;
  }
  dir = opendir(staging_directory);
  if (dir == NULL) {
    if (errno == ENOENT) {
      lc_free_with_allocator(&pouch->allocator, staging_directory);
      return LC_OK;
    }
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to open pouch history staging directory",
                      strerror(errno), staging_directory, "pouch");
    lc_free_with_allocator(&pouch->allocator, staging_directory);
    return rc;
  }
  removed = 0;
  rc = LC_OK;
  while (rc == LC_OK && (entry = readdir(dir)) != NULL) {
    char *path;
    struct stat st;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    path =
        lc_pouch_path_join(&pouch->allocator, staging_directory, entry->d_name);
    if (path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch history staging record", NULL,
                        staging_directory, "pouch");
    } else if (lstat(path, &st) != 0 || !S_ISREG(st.st_mode)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history staging entry is not a regular file",
                        NULL, path, "pouch");
    } else if (unlink(path) != 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to remove abandoned pouch history staging "
                        "record",
                        strerror(errno), path, "pouch");
    } else {
      removed = 1;
    }
    lc_free_with_allocator(&pouch->allocator, path);
  }
  if (closedir(dir) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch history staging directory",
                      strerror(errno), staging_directory, "pouch");
  }
  if (rc == LC_OK && removed) {
    rc = lc_pouch_path_fsync_directory(
        staging_directory, "failed to sync pouch history staging directory",
        error);
  }
  lc_free_with_allocator(&pouch->allocator, staging_directory);
  return rc;
}

static int lc_pouch_history_parse_ack(char *line, lc_index_seq *out,
                                      lc_error *error) {
  lc_u64 value;
  size_t length;

  if (line == NULL || out == NULL || strncmp(line, "ack=", 4U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history consumer acknowledgement is invalid",
                        NULL, NULL, "pouch");
  }
  length = strlen(line);
  if (length < 6U || line[length - 1U] != '\n') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history consumer acknowledgement is truncated",
                        NULL, NULL, "pouch");
  }
  line[length - 1U] = '\0';
  if (!lc_u64_parse_base10(line + 4U, &value)) {
    line[length - 1U] = '\n';
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history consumer acknowledgement is invalid",
                        NULL, NULL, "pouch");
  }
  line[length - 1U] = '\n';
  *out = (lc_index_seq)value;
  return LC_OK;
}

/* History controls are durable Pouch metadata, not caller-owned inputs.  Read
 * only regular files so a damaged control directory cannot redirect a cursor
 * operation through a symlink or special file. */
static int lc_pouch_history_validate_file(const char *path, int *exists,
                                          lc_error *error) {
  struct stat st;

  *exists = 0;
  if (lstat(path, &st) != 0) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to inspect pouch history consumer record",
                        strerror(errno), path, "pouch");
  }
  if (!S_ISREG(st.st_mode)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history consumer entry is not a regular file",
                        NULL, path, "pouch");
  }
  *exists = 1;
  return LC_OK;
}

static int lc_pouch_history_read(const char *path, int *found,
                                 lc_index_seq *acknowledged_index_seq,
                                 lc_error *error) {
  char magic[16];
  char ack[64];
  char extra[2];
  FILE *fp;
  int exists;
  int rc;

  *found = 0;
  *acknowledged_index_seq = 0U;
  exists = 0;
  rc = lc_pouch_history_validate_file(path, &exists, error);
  if (rc != LC_OK || !exists) {
    return rc;
  }
  fp = fopen(path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch history consumer record",
                        strerror(errno), path, "pouch");
  }
  rc = LC_OK;
  if (fgets(magic, sizeof(magic), fp) == NULL ||
      strcmp(magic, LC_POUCH_HISTORY_MAGIC "\n") != 0 ||
      fgets(ack, sizeof(ack), fp) == NULL) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch history consumer record is malformed", NULL, path,
                      "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_history_parse_ack(ack, acknowledged_index_seq, error);
  }
  if (rc == LC_OK && fgets(extra, sizeof(extra), fp) != NULL) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch history consumer record has trailing bytes", NULL,
                      path, "pouch");
  }
  if (ferror(fp) && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to read pouch history consumer record",
                      strerror(errno), path, "pouch");
  }
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch history consumer record",
                      strerror(errno), path, "pouch");
  }
  if (rc == LC_OK) {
    *found = 1;
  }
  return rc;
}

static int lc_pouch_history_write(lc_pouch *pouch, const char *directory,
                                  const char *path,
                                  lc_index_seq acknowledged_index_seq,
                                  lc_error *error) {
  char acknowledged[32];
  char text[64];
  char *staging_directory;
  char *staging_path;
  const char *leaf;
  int rc;

  if (lc_u64_format_base10((lc_u64)acknowledged_index_seq, acknowledged,
                           sizeof(acknowledged)) < 0 ||
      snprintf(text, sizeof(text), LC_POUCH_HISTORY_MAGIC "\nack=%s\n",
               acknowledged) < 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "failed to format pouch history consumer record", NULL,
                        NULL, "pouch");
  }
  if (strlen(text) >= sizeof(text)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history consumer record exceeds limit", NULL,
                        NULL, "pouch");
  }
  leaf = strrchr(path, '/');
  leaf = leaf != NULL ? leaf + 1 : path;
  staging_directory = NULL;
  staging_path = NULL;
  rc = lc_pouch_history_staging_directory(pouch, directory, 1,
                                          &staging_directory, error);
  if (rc == LC_OK) {
    staging_path =
        lc_pouch_path_join(&pouch->allocator, staging_directory, leaf);
    if (staging_path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch history staging record", NULL,
                        staging_directory, "pouch");
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_path_write_text_file(staging_path, text, error);
  }
  if (rc == LC_OK && rename(staging_path, path) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to publish pouch history consumer record",
                      strerror(errno), path, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_path_fsync_directory(
        directory, "failed to sync pouch history consumer directory", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_path_fsync_directory(
        staging_directory, "failed to sync pouch history staging directory",
        error);
  }
  lc_free_with_allocator(&pouch->allocator, staging_path);
  lc_free_with_allocator(&pouch->allocator, staging_directory);
  return rc;
}

static int lc_pouch_history_current_locked(lc_pouch *pouch, const char *ns,
                                           lc_index_seq *out, lc_error *error) {
  lc_pouch_generation current_index_seq;
  int rc;

  *out = 0U;
  current_index_seq = 0U;
  rc = lc_pouch_state_index_seq(pouch, ns, &current_index_seq, error);
  if (rc == LC_OK) {
    *out = (lc_index_seq)current_index_seq;
  }
  return rc;
}

int lc_pouch_history_oldest_acknowledged(lc_pouch *pouch, const char *ns,
                                         int *has_consumers,
                                         lc_pouch_generation *out,
                                         lc_error *error) {
  char *directory;
  DIR *dir;
  struct dirent *entry;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || has_consumers == NULL ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history acknowledgement requires namespace and "
                        "outputs",
                        NULL, NULL, "pouch");
  }
  *has_consumers = 0;
  *out = 0U;
  directory = NULL;
  rc = lc_pouch_history_directory(pouch, ns, 0, &directory, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_history_staging_cleanup(pouch, directory, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, directory);
    return rc;
  }
  dir = opendir(directory);
  if (dir == NULL) {
    if (errno == ENOENT) {
      lc_free_with_allocator(&pouch->allocator, directory);
      return LC_OK;
    }
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to open pouch history consumer directory",
                      strerror(errno), directory, "pouch");
    lc_free_with_allocator(&pouch->allocator, directory);
    return rc;
  }
  rc = LC_OK;
  while (rc == LC_OK && (entry = readdir(dir)) != NULL) {
    char *decoded;
    char *path;
    lc_index_seq acknowledged_index_seq;
    int found;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    if (strcmp(entry->d_name, ".staging") == 0) {
      continue;
    }
    decoded = lc_pouch_path_unescape_name(&pouch->allocator, entry->d_name);
    path = lc_pouch_path_join(&pouch->allocator, directory, entry->d_name);
    if (decoded == NULL || path == NULL) {
      lc_free_with_allocator(&pouch->allocator, decoded);
      lc_free_with_allocator(&pouch->allocator, path);
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history consumer directory contains an invalid "
                        "entry",
                        NULL, directory, "pouch");
      break;
    }
    found = 0;
    acknowledged_index_seq = 0U;
    rc = lc_pouch_history_read(path, &found, &acknowledged_index_seq, error);
    if (rc == LC_OK && found) {
      if (!*has_consumers || acknowledged_index_seq < (lc_index_seq)*out) {
        *out = (lc_pouch_generation)acknowledged_index_seq;
      }
      *has_consumers = 1;
    }
    lc_free_with_allocator(&pouch->allocator, decoded);
    lc_free_with_allocator(&pouch->allocator, path);
  }
  if (closedir(dir) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch history consumer directory",
                      strerror(errno), directory, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, directory);
  return rc;
}

static int lc_pouch_history_operation_locked(void *opaque, lc_error *error) {
  lc_pouch_history_operation_context *context;
  lc_history_consumer_handle *consumer;
  char *directory;
  char *path;
  lc_index_seq current_index_seq;
  lc_index_seq acknowledged_index_seq;
  int found;
  int rc;

  context = (lc_pouch_history_operation_context *)opaque;
  if (context == NULL || context->consumer == NULL ||
      context->consumer->client == NULL ||
      context->consumer->client->pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history consumer is unavailable", NULL, NULL,
                        "pouch");
  }
  consumer = context->consumer;
  directory = NULL;
  path = NULL;
  current_index_seq = 0U;
  acknowledged_index_seq = 0U;
  found = 0;
  rc = lc_pouch_history_current_locked(consumer->client->pouch, consumer->ns,
                                       &current_index_seq, error);
  if (rc == LC_OK) {
    rc = lc_pouch_history_file_path(
        consumer->client->pouch, consumer->ns, consumer->consumer_id,
        context->operation == LC_POUCH_HISTORY_REGISTER, &directory, &path,
        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_history_staging_cleanup(consumer->client->pouch, directory,
                                          error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_history_read(path, &found, &acknowledged_index_seq, error);
  }
  if (rc == LC_OK && context->operation == LC_POUCH_HISTORY_REGISTER &&
      !found) {
    acknowledged_index_seq = context->requested_acknowledged_index_seq ==
                                     LC_HISTORY_CONSUMER_START_AT_CURRENT
                                 ? current_index_seq
                                 : context->requested_acknowledged_index_seq;
    if (acknowledged_index_seq > current_index_seq) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "history consumer acknowledgement exceeds current "
                        "namespace sequence",
                        NULL, NULL, "pouch");
    } else {
      rc = lc_pouch_history_write(consumer->client->pouch, directory, path,
                                  acknowledged_index_seq, error);
      if (rc == LC_OK) {
        found = 1;
      }
    }
  }
  if (rc == LC_OK && context->operation == LC_POUCH_HISTORY_ADVANCE) {
    if (!found) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch history consumer is no longer registered", NULL,
                        NULL, "pouch");
    } else if (context->requested_acknowledged_index_seq <
               acknowledged_index_seq) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "history consumer acknowledgement cannot move backward",
                        NULL, NULL, "pouch");
    } else if (context->requested_acknowledged_index_seq > current_index_seq) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "history consumer acknowledgement exceeds current "
                        "namespace sequence",
                        NULL, NULL, "pouch");
    } else if (context->requested_acknowledged_index_seq >
               acknowledged_index_seq) {
      rc = lc_pouch_compaction_note_history_advanced(consumer->client->pouch,
                                                     consumer->ns, error);
      if (rc == LC_OK) {
        acknowledged_index_seq = context->requested_acknowledged_index_seq;
        rc = lc_pouch_history_write(consumer->client->pouch, directory, path,
                                    acknowledged_index_seq, error);
      }
    }
  }
  if (rc == LC_OK && context->operation == LC_POUCH_HISTORY_POSITION &&
      !found) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch history consumer is no longer registered", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK && context->operation == LC_POUCH_HISTORY_UNREGISTER) {
    if (found) {
      rc = lc_pouch_compaction_note_history_advanced(consumer->client->pouch,
                                                     consumer->ns, error);
      if (rc == LC_OK && unlink(path) != 0 && errno != ENOENT) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to remove pouch history consumer record",
                          strerror(errno), path, "pouch");
      }
      if (rc == LC_OK) {
        rc = lc_pouch_path_fsync_directory(
            directory, "failed to sync pouch history consumer directory",
            error);
      }
    }
    if (rc == LC_OK) {
      consumer->registered = 0;
    }
  }
  if (rc == LC_OK && context->operation != LC_POUCH_HISTORY_UNREGISTER &&
      context->out != NULL) {
    context->out->acknowledged_index_seq = acknowledged_index_seq;
    context->out->current_index_seq = current_index_seq;
  }
  lc_free_with_allocator(&consumer->client->allocator, directory);
  lc_free_with_allocator(&consumer->client->allocator, path);
  return rc;
}

static int lc_history_consumer_run(lc_history_consumer_handle *consumer,
                                   lc_pouch_history_operation operation,
                                   lc_index_seq acknowledged_index_seq,
                                   lc_history_consumer_position *out,
                                   lc_error *error) {
  lc_pouch_history_operation_context context;

  if (consumer == NULL || consumer->client == NULL ||
      consumer->client->pouch == NULL || !consumer->registered) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "history consumer is closed or unregistered", NULL,
                        NULL, "pouch");
  }
  if (out != NULL) {
    memset(out, 0, sizeof(*out));
  }
  memset(&context, 0, sizeof(context));
  context.consumer = consumer;
  context.operation = operation;
  context.requested_acknowledged_index_seq = acknowledged_index_seq;
  context.out = out;
  return lc_pouch_state_with_namespace_lock(
      consumer->client->pouch, consumer->ns, lc_pouch_history_operation_locked,
      &context, error);
}

static int
lc_history_consumer_position_method(lc_history_consumer *self,
                                    lc_history_consumer_position *out,
                                    lc_error *error) {
  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "history consumer position requires output", NULL, NULL,
                        NULL);
  }
  return lc_history_consumer_run((lc_history_consumer_handle *)self,
                                 LC_POUCH_HISTORY_POSITION, 0U, out, error);
}

static int lc_history_consumer_advance_method(
    lc_history_consumer *self, lc_index_seq acknowledged_index_seq,
    lc_history_consumer_position *out, lc_error *error) {
  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "history consumer advance requires output", NULL, NULL,
                        NULL);
  }
  return lc_history_consumer_run((lc_history_consumer_handle *)self,
                                 LC_POUCH_HISTORY_ADVANCE,
                                 acknowledged_index_seq, out, error);
}

static int lc_history_consumer_unregister_method(lc_history_consumer *self,
                                                 lc_error *error) {
  return lc_history_consumer_run((lc_history_consumer_handle *)self,
                                 LC_POUCH_HISTORY_UNREGISTER, 0U, NULL, error);
}

static void lc_history_consumer_close_method(lc_history_consumer *self) {
  lc_history_consumer_handle *consumer;
  lc_client_handle *client;
  lc_allocator allocator;

  if (self == NULL) {
    return;
  }
  consumer = (lc_history_consumer_handle *)self;
  client = consumer->client;
  memset(&allocator, 0, sizeof(allocator));
  if (client != NULL) {
    allocator = client->allocator;
  }
  lc_free_with_allocator(&allocator, consumer->ns);
  lc_free_with_allocator(&allocator, consumer->consumer_id);
  lc_free_with_allocator(&allocator, consumer);
  if (client != NULL) {
    lc_client_handle_release(client);
  }
}

int lc_client_new_history_consumer_method(
    lc_client *self, const lc_history_consumer_config *config,
    lc_history_consumer **out, lc_error *error) {
  lc_client_handle *client;

  if (self == NULL || config == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "new_history_consumer requires client, config, and "
                        "output",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  client = (lc_client_handle *)self;
  if (client->is_pouch) {
    return lc_pouch_client_new_history_consumer_method(self, config, out,
                                                       error);
  }
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "durable history consumers are currently supported only "
                      "by Pouch",
                      NULL, NULL, NULL);
}

int lc_pouch_client_new_history_consumer_method(
    lc_client *self, const lc_history_consumer_config *config,
    lc_history_consumer **out, lc_error *error) {
  lc_client_handle *client;
  lc_history_consumer_handle *consumer;
  lc_history_consumer_position position;
  int rc;

  if (self == NULL || config == NULL || out == NULL || config->ns == NULL ||
      config->ns[0] == '\0' || config->consumer_id == NULL ||
      config->consumer_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "new_history_consumer requires namespace and consumer "
                        "identity",
                        NULL, NULL, "pouch");
  }
  *out = NULL;
  client = (lc_client_handle *)self;
  if (!client->is_pouch || client->pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "history consumer requires an open Pouch client", NULL,
                        NULL, "pouch");
  }
  consumer = (lc_history_consumer_handle *)lc_client_calloc(client, 1U,
                                                            sizeof(*consumer));
  if (consumer == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate history consumer", NULL, NULL,
                        NULL);
  }
  consumer->client = client;
  consumer->ns = lc_client_strdup(client, config->ns);
  consumer->consumer_id = lc_client_strdup(client, config->consumer_id);
  if (consumer->ns == NULL || consumer->consumer_id == NULL) {
    lc_client_free(client, consumer->ns);
    lc_client_free(client, consumer->consumer_id);
    lc_client_free(client, consumer);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy history consumer identity", NULL, NULL,
                        NULL);
  }
  consumer->registered = 1;
  consumer->pub.position = lc_history_consumer_position_method;
  consumer->pub.advance = lc_history_consumer_advance_method;
  consumer->pub.unregister = lc_history_consumer_unregister_method;
  consumer->pub.close = lc_history_consumer_close_method;
  consumer->pub.impl = consumer;
  lc_client_handle_retain(client);
  memset(&position, 0, sizeof(position));
  rc = lc_history_consumer_run(consumer, LC_POUCH_HISTORY_REGISTER,
                               config->initial_acknowledged_index_seq,
                               &position, error);
  if (rc != LC_OK) {
    lc_history_consumer_close_method(&consumer->pub);
    return rc;
  }
  *out = &consumer->pub;
  return LC_OK;
}
