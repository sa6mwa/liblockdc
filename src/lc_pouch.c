#include "lc_pouch.h"

#include "lc_api_internal.h"
#include "lc_pouch_format.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"
#include "lc_pouch_query_index.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static unsigned long lc_pouch_next_writer_marker_id;
static pthread_mutex_t lc_pouch_writer_marker_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t lc_pouch_root_manifest_mutex = PTHREAD_MUTEX_INITIALIZER;

static const char *lc_pouch_option_string(const char *value,
                                          const char *fallback) {
  if (value != NULL && value[0] != '\0') {
    return value;
  }
  return fallback;
}

static void lc_pouch_init_options(lc_pouch *pouch,
                                  const lc_pouch_open_options *options) {
  pouch->segment_target_bytes =
      options != NULL && options->segment_target_bytes != 0UL
          ? options->segment_target_bytes
          : LC_POUCH_DEFAULT_SEGMENT_TARGET_BYTES;
  pouch->compaction_min_segment_count =
      options != NULL && options->compaction_min_segment_count != 0UL
          ? options->compaction_min_segment_count
          : LC_POUCH_DEFAULT_COMPACTION_MIN_SEGMENT_COUNT;
  pouch->compaction_min_reclaimable_bytes =
      options != NULL && options->compaction_min_reclaimable_bytes != 0UL
          ? options->compaction_min_reclaimable_bytes
          : LC_POUCH_DEFAULT_COMPACTION_MIN_RECLAIMABLE_BYTES;
  pouch->compaction_interval_seconds =
      options != NULL ? options->compaction_interval_seconds : 0UL;
  pouch->background_compaction_enabled =
      options != NULL ? options->background_compaction_enabled : 0;
  pouch->single_writer = options != NULL ? options->single_writer : 0;
  pouch->query_engine = lc_strdup_with_allocator(
      &pouch->allocator,
      lc_pouch_option_string(options != NULL ? options->query_engine : NULL,
                             "index"));
  pouch->query_fallback_engine = lc_strdup_with_allocator(
      &pouch->allocator,
      lc_pouch_option_string(
          options != NULL ? options->query_fallback_engine : NULL, ""));
}

static int lc_pouch_write_root_manifest(lc_pouch *pouch, lc_error *error) {
  char manifest[512];
  char *crypto_key_id;
  char *manifest_path;
  int rc;

  crypto_key_id = NULL;
  if (lc_pouch_crypto_enabled(pouch->crypto)) {
    rc = lc_pouch_crypto_key_id(pouch->crypto, &crypto_key_id, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  manifest_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "manifest");
  if (manifest_path == NULL) {
    lc_free_with_allocator(NULL, crypto_key_id);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch root manifest path", NULL,
                        NULL, NULL);
  }
  snprintf(manifest, sizeof(manifest),
           "layout=%s\nversion=%lu\nsegment_target_bytes=%lu\n"
           "compaction_min_segment_count=%lu\n"
           "compaction_min_reclaimable_bytes=%lu\ncrypto=%s\n"
           "crypto_key_id=%s\n",
           LC_POUCH_LAYOUT_NAME, LC_POUCH_LAYOUT_VERSION,
           pouch->segment_target_bytes, pouch->compaction_min_segment_count,
           pouch->compaction_min_reclaimable_bytes,
           lc_pouch_crypto_enabled(pouch->crypto) ? "encrypted" : "plaintext",
           crypto_key_id != NULL ? crypto_key_id : "");
  rc = lc_pouch_path_write_text_file(manifest_path, manifest, error);
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  lc_free_with_allocator(NULL, crypto_key_id);
  return rc;
}

static int lc_pouch_root_manifest_read(lc_pouch *pouch, char *layout,
                                       size_t layout_size,
                                       unsigned long *version, char *mode,
                                       size_t mode_size, char *key_id,
                                       size_t key_id_size, int *found_manifest,
                                       int *found_crypto_mode,
                                       lc_error *error) {
  char line[512];
  char *manifest_path;
  FILE *fp;
  int found_key_id;
  int found_layout;
  int found_mode;
  int found_version;
  int rc;

  if (layout == NULL || layout_size == 0U || version == NULL || mode == NULL ||
      mode_size == 0U || key_id == NULL || key_id_size == 0U ||
      found_manifest == NULL || found_crypto_mode == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root manifest read requires outputs", NULL, NULL,
                        NULL);
  }
  layout[0] = '\0';
  *version = 0UL;
  mode[0] = '\0';
  key_id[0] = '\0';
  *found_manifest = 0;
  *found_crypto_mode = 0;
  manifest_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "manifest");
  if (manifest_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch root manifest path", NULL,
                        NULL, NULL);
  }
  fp = fopen(manifest_path, "rb");
  if (fp == NULL) {
    rc = errno == ENOENT ? LC_OK
                         : lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                                        "failed to open pouch root manifest",
                                        strerror(errno), NULL, "pouch");
    lc_free_with_allocator(&pouch->allocator, manifest_path);
    return rc;
  }
  *found_manifest = 1;
  found_layout = 0;
  found_version = 0;
  found_mode = 0;
  found_key_id = 0;
  rc = LC_OK;
  while (fgets(line, sizeof(line), fp) != NULL) {
    char *value;
    size_t len;

    if (strncmp(line, "layout=", 7U) == 0) {
      if (found_layout) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest has duplicate layout", NULL,
                          NULL, "pouch");
        break;
      }
      value = line + 7U;
      len = strcspn(value, "\r\n");
      value[len] = '\0';
      if (len == 0U || len + 1U > layout_size) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest layout is invalid", NULL, NULL,
                          "pouch");
        break;
      }
      memcpy(layout, value, len + 1U);
      found_layout = 1;
      continue;
    }
    if (strncmp(line, "version=", 8U) == 0) {
      char *end;
      unsigned long parsed;

      if (found_version) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest has duplicate version", NULL,
                          NULL, "pouch");
        break;
      }
      value = line + 8U;
      len = strcspn(value, "\r\n");
      value[len] = '\0';
      end = NULL;
      parsed = strtoul(value, &end, 10);
      if (end == value || (end != NULL && *end != '\0') || parsed == 0UL) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest version is invalid", NULL, NULL,
                          "pouch");
        break;
      }
      *version = parsed;
      found_version = 1;
      continue;
    }
    if (strncmp(line, "crypto=", 7U) == 0) {
      if (found_mode) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest has duplicate crypto mode", NULL,
                          NULL, "pouch");
        break;
      }
      value = line + 7U;
      len = strcspn(value, "\r\n");
      value[len] = '\0';
      if (strcmp(value, "plaintext") != 0 && strcmp(value, "encrypted") != 0) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest has invalid crypto mode", NULL,
                          NULL, "pouch");
        break;
      }
      if (len + 1U > mode_size) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest crypto mode is too long", NULL,
                          NULL, "pouch");
        break;
      }
      memcpy(mode, value, len + 1U);
      found_mode = 1;
      continue;
    }
    if (strncmp(line, "crypto_key_id=", 14U) == 0) {
      if (found_key_id) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest has duplicate crypto key id",
                          NULL, NULL, "pouch");
        break;
      }
      value = line + 14U;
      len = strcspn(value, "\r\n");
      value[len] = '\0';
      if (len + 1U > key_id_size) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest crypto key id is too long", NULL,
                          NULL, "pouch");
        break;
      }
      memcpy(key_id, value, len + 1U);
      found_key_id = 1;
    }
  }
  if (ferror(fp) && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to read pouch root manifest", strerror(errno),
                      NULL, "pouch");
  }
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch root manifest", strerror(errno),
                      NULL, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  if (rc == LC_OK && !found_layout) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch root manifest requires layout", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK && !found_version) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch root manifest requires version", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK && !found_mode) {
    (void)snprintf(mode, mode_size, "plaintext");
  }
  if (rc == LC_OK) {
    *found_crypto_mode = found_mode;
  }
  return rc;
}

static int lc_pouch_root_has_namespace_entries(lc_pouch *pouch, int *out,
                                               lc_error *error) {
  char *namespaces_path;
  DIR *dir;
  struct dirent *entry;
  int saved_errno;

  if (pouch == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root namespace scan requires pouch and output",
                        NULL, NULL, NULL);
  }
  *out = 0;
  namespaces_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "namespaces");
  if (namespaces_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespaces path", NULL, NULL,
                        NULL);
  }
  dir = opendir(namespaces_path);
  if (dir == NULL) {
    saved_errno = errno;
    lc_free_with_allocator(&pouch->allocator, namespaces_path);
    if (saved_errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to scan pouch namespaces", strerror(saved_errno),
                        NULL, "pouch");
  }
  errno = 0;
  while ((entry = readdir(dir)) != NULL) {
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    *out = 1;
    break;
  }
  saved_errno = errno;
  if (closedir(dir) != 0 && saved_errno == 0) {
    saved_errno = errno != 0 ? errno : EIO;
  }
  lc_free_with_allocator(&pouch->allocator, namespaces_path);
  if (saved_errno != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to scan pouch namespaces", strerror(saved_errno),
                        NULL, "pouch");
  }
  return LC_OK;
}

static int lc_pouch_validate_root_crypto_mode(lc_pouch *pouch,
                                              lc_error *error) {
  char stored_key_id[128];
  char stored_layout[64];
  char stored_mode[sizeof("encrypted")];
  char *requested_key_id;
  const char *requested_mode;
  unsigned long stored_version;
  int found_crypto_mode;
  int found_manifest;
  int has_namespace_entries;
  int rc;

  requested_key_id = NULL;
  requested_mode =
      lc_pouch_crypto_enabled(pouch->crypto) ? "encrypted" : "plaintext";
  stored_version = 0UL;
  found_crypto_mode = 0;
  rc = lc_pouch_root_manifest_read(
      pouch, stored_layout, sizeof(stored_layout), &stored_version, stored_mode,
      sizeof(stored_mode), stored_key_id, sizeof(stored_key_id),
      &found_manifest, &found_crypto_mode, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!found_manifest) {
    has_namespace_entries = 0;
    rc = lc_pouch_root_has_namespace_entries(pouch, &has_namespace_entries,
                                             error);
    if (rc != LC_OK) {
      return rc;
    }
    if (has_namespace_entries) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest is required for populated root",
                          NULL, NULL, "pouch");
    }
    return LC_OK;
  }
  if (strcmp(stored_layout, LC_POUCH_LAYOUT_NAME) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root manifest layout is unsupported", NULL, NULL,
                        "pouch");
  }
  if (stored_version != LC_POUCH_LAYOUT_VERSION) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root manifest version is unsupported", NULL,
                        NULL, "pouch");
  }
  if (!found_crypto_mode) {
    has_namespace_entries = 0;
    rc = lc_pouch_root_has_namespace_entries(pouch, &has_namespace_entries,
                                             error);
    if (rc != LC_OK) {
      return rc;
    }
    if (has_namespace_entries) {
      return lc_error_set(
          error, LC_ERR_INVALID, 0L,
          "pouch root manifest requires crypto mode for populated root", NULL,
          NULL, "pouch");
    }
    if (stored_key_id[0] != '\0') {
      return lc_error_set(
          error, LC_ERR_INVALID, 0L,
          "pouch root manifest crypto key id requires crypto mode", NULL, NULL,
          "pouch");
    }
    return LC_OK;
  }
  if (strcmp(stored_mode, requested_mode) != 0) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch root crypto mode cannot be changed after initialization", NULL,
        NULL, "pouch");
  }
  if (strcmp(stored_mode, "plaintext") == 0 && stored_key_id[0] != '\0') {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch plaintext root manifest cannot contain crypto key id", NULL,
        NULL, "pouch");
  }
  if (strcmp(stored_mode, "encrypted") != 0) {
    return LC_OK;
  }
  if (stored_key_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted root manifest requires crypto key id",
                        NULL, NULL, "pouch");
  }
  rc = lc_pouch_crypto_key_id(pouch->crypto, &requested_key_id, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (strcmp(stored_key_id, requested_key_id) != 0) {
    lc_free_with_allocator(NULL, requested_key_id);
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch root crypto key does not match initialized encrypted root", NULL,
        NULL, "pouch");
  }
  lc_free_with_allocator(NULL, requested_key_id);
  return LC_OK;
}

static int lc_pouch_root_manifest_lock(lc_pouch *pouch, int *out,
                                       lc_error *error) {
  char *lock_path;
  struct flock fl;
  int fd;

  if (pouch == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root manifest lock requires pouch and output",
                        NULL, NULL, NULL);
  }
  *out = -1;
  lock_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "manifest.lock");
  if (lock_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch root manifest lock path",
                        NULL, NULL, NULL);
  }
  fd = open(lock_path, O_CREAT | O_RDWR, 0666);
  lc_free_with_allocator(&pouch->allocator, lock_path);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch root manifest lock",
                        strerror(errno), NULL, "pouch");
  }
  memset(&fl, 0, sizeof(fl));
  fl.l_type = F_WRLCK;
  fl.l_whence = SEEK_SET;
  while (fcntl(fd, F_SETLKW, &fl) != 0) {
    if (errno == EINTR) {
      continue;
    }
    close(fd);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch root manifest", strerror(errno),
                        NULL, "pouch");
  }
  *out = fd;
  return LC_OK;
}

static void lc_pouch_root_manifest_unlock(int fd) {
  struct flock fl;

  if (fd < 0) {
    return;
  }
  memset(&fl, 0, sizeof(fl));
  fl.l_type = F_UNLCK;
  fl.l_whence = SEEK_SET;
  (void)fcntl(fd, F_SETLK, &fl);
  close(fd);
}

static int lc_pouch_ensure_root(lc_pouch *pouch, lc_error *error) {
  char *namespaces_path;
  int manifest_mutex_locked;
  int lock_fd;
  int pthread_rc;
  int rc;

  rc = lc_pouch_path_ensure_directory(
      pouch->root_path, "failed to create pouch root directory", error);
  if (rc != LC_OK) {
    return rc;
  }
  namespaces_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "namespaces");
  if (namespaces_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespaces path", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_path_ensure_directory(
      namespaces_path, "failed to create pouch namespaces directory", error);
  lc_free_with_allocator(&pouch->allocator, namespaces_path);
  if (rc != LC_OK) {
    return rc;
  }
  manifest_mutex_locked = 0;
  lock_fd = -1;
  pthread_rc = pthread_mutex_lock(&lc_pouch_root_manifest_mutex);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch root manifest mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  manifest_mutex_locked = 1;
  rc = lc_pouch_root_manifest_lock(pouch, &lock_fd, error);
  if (rc == LC_OK) {
    rc = lc_pouch_validate_root_crypto_mode(pouch, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_write_root_manifest(pouch, error);
  }
  lc_pouch_root_manifest_unlock(lock_fd);
  if (manifest_mutex_locked) {
    pthread_mutex_unlock(&lc_pouch_root_manifest_mutex);
  }
  return rc;
}

static int lc_pouch_init_writer_marker(lc_pouch *pouch, lc_error *error) {
  char leaf[128];
  unsigned long writer_id;

  pthread_mutex_lock(&lc_pouch_writer_marker_mutex);
  writer_id = ++lc_pouch_next_writer_marker_id;
  pthread_mutex_unlock(&lc_pouch_writer_marker_mutex);
  snprintf(leaf, sizeof(leaf), "writer-%ld-%020lu.marker", (long)getpid(),
           writer_id);
  pouch->writer_marker_leaf = lc_strdup_with_allocator(&pouch->allocator, leaf);
  if (pouch->writer_marker_leaf == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch writer marker leaf", NULL,
                        NULL, NULL);
  }
  return LC_OK;
}

int lc_pouch_open(const char *root_path, const lc_allocator *allocator,
                  const lc_pouch_open_options *options, lc_pouch **out,
                  lc_error *error) {
  lc_pouch *pouch;
  lc_pouch_crypto_open_options crypto_options;
  int rc;

  if (root_path == NULL || root_path[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_open requires root_path and out", NULL, NULL,
                        NULL);
  }
  *out = NULL;
  pouch = (lc_pouch *)lc_calloc_with_allocator(allocator, 1U, sizeof(*pouch));
  if (pouch == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch handle", NULL, NULL, NULL);
  }
  if (allocator != NULL) {
    pouch->allocator = *allocator;
  } else {
    lc_allocator_init(&pouch->allocator);
  }
  pouch->root_path = lc_strdup_with_allocator(&pouch->allocator, root_path);
  if (pouch->root_path == NULL) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch root path", NULL, NULL, NULL);
  }
  lc_pouch_init_options(pouch, options);
  if (pouch->query_engine == NULL || pouch->query_fallback_engine == NULL) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query options", NULL, NULL,
                        NULL);
  }
  if (strcmp(pouch->query_engine, "index") != 0 &&
      strcmp(pouch->query_engine, "scan") != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_engine must be index or scan", NULL, NULL,
                        "pouch");
  }
  if (pouch->query_fallback_engine[0] != '\0' &&
      strcmp(pouch->query_fallback_engine, "index") != 0 &&
      strcmp(pouch->query_fallback_engine, "scan") != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_fallback_engine must be index or scan",
                        NULL, NULL, "pouch");
  }
  memset(&crypto_options, 0, sizeof(crypto_options));
  if (options != NULL) {
    crypto_options.key_string = options->crypto_key;
    crypto_options.key_file = options->crypto_key_file;
    crypto_options.generate_key_file = options->crypto_generate_key_file;
  }
  rc = lc_pouch_crypto_open(&pouch->allocator, &crypto_options, &pouch->crypto,
                            &pouch->crypto_key_file, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_init_writer_marker(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_ensure_root(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  *out = pouch;
  return LC_OK;
}

void lc_pouch_close(lc_pouch *pouch) {
  lc_allocator allocator;

  if (pouch == NULL) {
    return;
  }
  allocator = pouch->allocator;
  lc_pouch_query_index_prepared_cache_cleanup(pouch);
  lc_pouch_state_cache_cleanup(pouch);
  lc_pouch_crypto_close(pouch->crypto);
  lc_free_with_allocator(&allocator, pouch->crypto_key_file);
  lc_free_with_allocator(&allocator, pouch->writer_marker_leaf);
  lc_free_with_allocator(&allocator, pouch->query_fallback_engine);
  lc_free_with_allocator(&allocator, pouch->query_engine);
  lc_free_with_allocator(&allocator, pouch->root_path);
  lc_free_with_allocator(&allocator, pouch);
}

int lc_pouch_status_read(lc_pouch *pouch, lc_pouch_status *out,
                         lc_error *error) {
  if (pouch == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_status_read requires pouch and out", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  out->root_path =
      lc_strdup_with_allocator(&pouch->allocator, pouch->root_path);
  out->layout_name =
      lc_strdup_with_allocator(&pouch->allocator, LC_POUCH_LAYOUT_NAME);
  if (out->root_path == NULL || out->layout_name == NULL) {
    lc_pouch_status_cleanup(&pouch->allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to copy pouch status",
                        NULL, NULL, NULL);
  }
  out->layout_version = LC_POUCH_LAYOUT_VERSION;
  out->segment_target_bytes = pouch->segment_target_bytes;
  out->compaction_min_segment_count = pouch->compaction_min_segment_count;
  out->compaction_min_reclaimable_bytes =
      pouch->compaction_min_reclaimable_bytes;
  out->compaction_interval_seconds = pouch->compaction_interval_seconds;
  out->background_compaction_enabled = pouch->background_compaction_enabled;
  out->single_writer = pouch->single_writer;
  out->query_engine =
      lc_strdup_with_allocator(&pouch->allocator, pouch->query_engine);
  out->query_fallback_engine =
      lc_strdup_with_allocator(&pouch->allocator, pouch->query_fallback_engine);
  out->crypto_enabled = lc_pouch_crypto_enabled(pouch->crypto);
  out->crypto_key_file =
      pouch->crypto_key_file != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, pouch->crypto_key_file)
          : NULL;
  if (out->query_engine == NULL || out->query_fallback_engine == NULL) {
    lc_pouch_status_cleanup(&pouch->allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch query status", NULL, NULL, NULL);
  }
  if (pouch->crypto_key_file != NULL && out->crypto_key_file == NULL) {
    lc_pouch_status_cleanup(&pouch->allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch crypto key file status", NULL,
                        NULL, NULL);
  }
  return LC_OK;
}

void lc_pouch_status_cleanup(const lc_allocator *allocator,
                             lc_pouch_status *status) {
  if (status == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, status->root_path);
  lc_free_with_allocator(allocator, status->layout_name);
  lc_free_with_allocator(allocator, status->query_engine);
  lc_free_with_allocator(allocator, status->query_fallback_engine);
  lc_free_with_allocator(allocator, status->crypto_key_file);
  memset(status, 0, sizeof(*status));
}

int lc_pouch_ensure_namespace(lc_pouch *pouch, const char *namespace_name,
                              lc_error *error) {
  int rc;

  if (pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_ensure_namespace requires pouch", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_namespace_ensure(&pouch->allocator, pouch->root_path,
                                 namespace_name, error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_state_recover_staged_decisions(pouch, namespace_name, error);
}
