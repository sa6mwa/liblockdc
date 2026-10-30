#include <cmocka.h>
#include <fcntl.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../support/lc_test_tmp.h"
#include "lc_api_internal.h"
#include "lc_collection_ids.h"

#define PREFIX LC_COLLECTION_TMP_PARENT "/collection-ids-"

typedef struct fixture {
  char path[512];
  lc_collection_ids ids;
  lc_error error;
} fixture;

static int setup(void **state) {
  fixture *f = calloc(1, sizeof(*f));
  int fd;
  assert_non_null(f);
  f->ids.fd = -1;
  strcpy(f->path, PREFIX "XXXXXX");
  fd = lc_test_tmp_mkstemp(f->path, PREFIX);
  assert_true(fd >= 0);
  assert_int_equal(close(fd), 0);
  lc_error_init(&f->error);
  *state = f;
  assert_int_equal(lc_collection_ids_open(f->path, 1, NULL, &f->ids, &f->error),
                   LC_OK);
  return 0;
}

static int teardown(void **state) {
  fixture *f = *state;
  lc_collection_ids_close(&f->ids);
  lc_error_cleanup(&f->error);
  lc_test_tmp_cleanup_path(f->path, PREFIX);
  free(f);
  return 0;
}

static lc_collection_id_ref reference(lc_u64 position) {
  lc_collection_id_ref ref;
  memset(&ref, 0, sizeof(ref));
  for (size_t i = 0; i < sizeof(ref.digest); ++i)
    ref.digest[i] = (unsigned char)(position >> (8 * (i % 8)));
  ref.position = position;
  ref.generation = 1;
  ref.segment = 1;
  ref.offset = position;
  return ref;
}

static int position_match(void *context, const lc_collection_id_ref *candidate,
                          int *matched, lc_error *error) {
  (void)error;
  *matched = candidate->position == *(lc_u64 *)context;
  return LC_OK;
}

static int failing_match(void *context, const lc_collection_id_ref *candidate,
                         int *matched, lc_error *error) {
  (void)context;
  (void)candidate;
  (void)matched;
  return lc_error_set(error, LC_ERR_TIMEOUT, 0, "canonical lookup failed", NULL,
                      NULL, NULL);
}

static void test_grow_reopen_and_peer_refresh(void **state) {
  fixture *f = *state;
  lc_collection_ids peer;
  lc_collection_id_ref ref, found_ref;
  int found;
  assert_int_equal(lc_collection_ids_open(f->path, 0, NULL, &peer, &f->error),
                   LC_OK);
  for (lc_u64 position = 1; position <= 1000; ++position) {
    ref = reference(position);
    assert_int_equal(lc_collection_ids_put(&f->ids, &ref, &f->error), LC_OK);
  }
  assert_true(f->ids.capacity >= 2000 && f->ids.count == 1000);
  assert_int_equal(
      lc_collection_ids_checkpoint(&f->ids, 1000, 1000, 2, 500, &f->error),
      LC_OK);
  assert_int_equal(lc_collection_ids_refresh(&peer, &f->error), LC_OK);
  assert_true(peer.count == 1000 && peer.replay_segment == 2 &&
              peer.replay_offset == 500);
  for (lc_u64 position = 1; position <= 1000; position += 7) {
    ref = reference(position);
    assert_int_equal(lc_collection_ids_find(&peer, ref.digest, position_match,
                                            &position, &found_ref, &found,
                                            &f->error),
                     LC_OK);
    assert_true(found && found_ref.position == position &&
                found_ref.offset == position);
  }
  lc_collection_ids_close(&peer);
  lc_collection_ids_close(&f->ids);
  assert_int_equal(lc_collection_ids_open(f->path, 0, NULL, &f->ids, &f->error),
                   LC_OK);
  ref = reference(1001);
  assert_int_equal(lc_collection_ids_find(&f->ids, ref.digest, position_match,
                                          &ref.position, &found_ref, &found,
                                          &f->error),
                   LC_OK);
  assert_false(found);
}

static void test_digest_collisions_and_full_width_identity(void **state) {
  fixture *f = *state;
  lc_collection_id_ref ref, found_ref;
  const lc_u64 positions[] = {1, (lc_u64)LC_I64_MAX + 4, LC_U64_MAX};
  int found;
  for (size_t i = 0; i < 3; ++i) {
    ref = reference(positions[i]);
    memset(ref.digest, 0, sizeof(ref.digest));
    assert_int_equal(lc_collection_ids_put(&f->ids, &ref, &f->error), LC_OK);
  }
  for (size_t i = 0; i < 3; ++i) {
    lc_u64 position = positions[i];
    assert_int_equal(lc_collection_ids_find(&f->ids, ref.digest, position_match,
                                            &position, &found_ref, &found,
                                            &f->error),
                     LC_OK);
    assert_true(found && found_ref.position == position);
  }
  assert_true(f->ids.count == 3);
  assert_int_equal(lc_collection_ids_find(&f->ids, ref.digest, failing_match,
                                          NULL, &found_ref, &found, &f->error),
                   LC_ERR_TIMEOUT);
  assert_false(found);
  assert_string_equal(f->error.message, "canonical lookup failed");
}

static void test_replay_preserves_deletion_and_allows_relocation(void **state) {
  fixture *f = *state;
  lc_collection_id_ref ref = reference(10), found_ref;
  int found;
  assert_int_equal(lc_collection_ids_put(&f->ids, &ref, &f->error), LC_OK);
  ref.generation = 7;
  ref.offset = 100;
  assert_int_equal(lc_collection_ids_put(&f->ids, &ref, &f->error), LC_OK);
  ref.generation = 1;
  ref.offset = 10;
  assert_int_equal(lc_collection_ids_put(&f->ids, &ref, &f->error), LC_OK);
  assert_int_equal(lc_collection_ids_find(&f->ids, ref.digest, position_match,
                                          &ref.position, &found_ref, &found,
                                          &f->error),
                   LC_OK);
  assert_true(found && found_ref.generation == 7 && found_ref.offset == 100);
  ref.generation = 7;
  ref.segment = 2;
  ref.offset = 0;
  assert_int_equal(lc_collection_ids_put(&f->ids, &ref, &f->error), LC_OK);
  assert_int_equal(lc_collection_ids_find(&f->ids, ref.digest, position_match,
                                          &ref.position, &found_ref, &found,
                                          &f->error),
                   LC_OK);
  assert_true(found && found_ref.segment == 2 && found_ref.offset == 0);
  assert_true(f->ids.count == 1);
}

static void
test_corrupt_header_slot_and_truncation_fail_explicitly(void **state) {
  fixture *f = *state;
  lc_collection_id_ref ref = reference(1), found_ref;
  lc_collection_ids reopened;
  int found;
  unsigned char byte;
  const off_t offset = 64 + 80 + 8;
  assert_int_equal(lc_collection_ids_put(&f->ids, &ref, &f->error), LC_OK);
  assert_int_equal(pread(f->ids.fd, &byte, 1, offset), 1);
  byte ^= 1;
  assert_int_equal(pwrite(f->ids.fd, &byte, 1, offset), 1);
  assert_int_equal(lc_collection_ids_find(&f->ids, ref.digest, position_match,
                                          &ref.position, &found_ref, &found,
                                          &f->error),
                   LC_ERR_PROTOCOL);
  assert_false(found);
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  /* Healthy cold open validates only the header; accessed slots prove their
   * CRC. */
  assert_int_equal(
      lc_collection_ids_open(f->path, 0, NULL, &reopened, &f->error), LC_OK);
  lc_collection_ids_close(&reopened);
  assert_int_equal(ftruncate(f->ids.fd, 100), 0);
  assert_int_equal(
      lc_collection_ids_open(f->path, 1, NULL, &reopened, &f->error),
      LC_ERR_PROTOCOL);
  assert_int_equal(reopened.fd, -1);
}

static void test_binding_updates_do_not_resize_at_load_threshold(void **state) {
  fixture *f = *state;
  lc_collection_id_ref ref;
  for (lc_u64 position = 1; position <= 32; ++position) {
    ref = reference(position);
    assert_int_equal(lc_collection_ids_put(&f->ids, &ref, &f->error), LC_OK);
  }
  assert_true(f->ids.capacity == 64 && f->ids.count == 32);
  for (lc_u64 position = 1; position <= 32; ++position) {
    ref = reference(position);
    ref.generation = 10;
    ref.offset = 10000 + position;
    assert_int_equal(lc_collection_ids_put(&f->ids, &ref, &f->error), LC_OK);
  }
  assert_true(f->ids.capacity == 64 && f->ids.count == 32);
  ref = reference(33);
  assert_int_equal(lc_collection_ids_put(&f->ids, &ref, &f->error), LC_OK);
  assert_true(f->ids.capacity == 128 && f->ids.count == 33);
}

static void test_staged_entries_publish_only_with_checkpoint(void **state) {
  fixture *f = *state;
  lc_collection_id_ref ref = reference(1);
  unsigned char before[64], staged[64];
  assert_int_equal(pread(f->ids.fd, before, sizeof(before), 0), sizeof(before));
  assert_int_equal(lc_collection_ids_put(&f->ids, &ref, &f->error), LC_OK);
  assert_int_equal(pread(f->ids.fd, staged, sizeof(staged), 0), sizeof(staged));
  /* A slot update must not publish an intermediate header that the following
   * canonical replay checkpoint immediately replaces. */
  assert_memory_equal(before, staged, sizeof(before));
  assert_int_equal(
      lc_collection_ids_checkpoint(&f->ids, 1, 1, 2, 500, &f->error), LC_OK);
  lc_collection_ids_close(&f->ids);
  assert_int_equal(lc_collection_ids_open(f->path, 0, NULL, &f->ids, &f->error),
                   LC_OK);
  assert_true(f->ids.count == 1 && f->ids.generation == 1 &&
              f->ids.replay_segment == 2 && f->ids.replay_offset == 500);
}

static void test_failed_checkpoint_preserves_replay_boundary(void **state) {
  fixture *f = *state;
  lc_collection_id_ref ref = reference(1), observed;
  int found;
  assert_int_equal(
      lc_collection_ids_checkpoint(&f->ids, 0, 0, 1, 100, &f->error), LC_OK);
  assert_int_equal(lc_collection_ids_put(&f->ids, &ref, &f->error), LC_OK);
  assert_int_equal(close(f->ids.fd), 0);
  f->ids.fd = open(f->path, O_RDONLY);
  assert_true(f->ids.fd >= 0);
  assert_int_equal(
      lc_collection_ids_checkpoint(&f->ids, 1, 1, 1, 200, &f->error),
      LC_ERR_TRANSPORT);
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  lc_collection_ids_close(&f->ids);
  assert_int_equal(lc_collection_ids_open(f->path, 0, NULL, &f->ids, &f->error),
                   LC_OK);
  assert_true(f->ids.count == 0 && f->ids.generation == 0 &&
              f->ids.replay_segment == 1 && f->ids.replay_offset == 100);
  /* Replay sees the already-written slot and the authoritative count repairs
   * its header without duplicating the identity or advancing the wrong tail. */
  assert_int_equal(lc_collection_ids_put(&f->ids, &ref, &f->error), LC_OK);
  assert_int_equal(
      lc_collection_ids_checkpoint(&f->ids, 1, 1, 1, 200, &f->error), LC_OK);
  assert_int_equal(lc_collection_ids_find(&f->ids, ref.digest, position_match,
                                          &ref.position, &observed, &found,
                                          &f->error),
                   LC_OK);
  assert_true(found && observed.position == 1 && f->ids.count == 1);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test_setup_teardown(
          test_staged_entries_publish_only_with_checkpoint, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_failed_checkpoint_preserves_replay_boundary, setup, teardown),
      cmocka_unit_test_setup_teardown(test_grow_reopen_and_peer_refresh, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(
          test_digest_collisions_and_full_width_identity, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_replay_preserves_deletion_and_allows_relocation, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_corrupt_header_slot_and_truncation_fail_explicitly, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_binding_updates_do_not_resize_at_load_threshold, setup,
          teardown),
  };
  if (lc_allocator_tracking_configure(1, NULL) != LC_OK)
    return 1;
  return cmocka_run_group_tests(tests, NULL, NULL);
}
