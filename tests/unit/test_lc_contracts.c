#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>

#include "lc/lc.h"
#include "lc_api_internal.h"

typedef struct fake_client {
  lc_client pub;
  int acquire_calls;
  int close_calls;
  lc_lease *lease_to_return;
} fake_client;

typedef struct fake_lease {
  lc_lease pub;
  int release_calls;
  int close_calls;
} fake_lease;

typedef struct fake_message {
  lc_message pub;
  int write_payload_calls;
  int close_calls;
  lc_lease *state_handle;
  lc_source *payload_handle;
  size_t payload_size;
} fake_message;

static int fake_client_acquire(lc_client *self, const lc_acquire_req *req,
                               lc_lease **out, lc_error *error) {
  fake_client *client;

  (void)req;
  (void)error;
  client = (fake_client *)self;
  client->acquire_calls += 1;
  *out = client->lease_to_return;
  return LC_OK;
}

static void fake_client_close(lc_client *self) {
  fake_client *client;

  client = (fake_client *)self;
  client->close_calls += 1;
}

static int fake_lease_release(lc_lease *self, const lc_release_req *req,
                              lc_error *error) {
  fake_lease *lease;

  (void)req;
  (void)error;
  lease = (fake_lease *)self;
  lease->release_calls += 1;
  return LC_OK;
}

static void fake_lease_close(lc_lease *self) {
  fake_lease *lease;

  lease = (fake_lease *)self;
  lease->close_calls += 1;
}

static lc_source *fake_message_payload_reader(lc_message *self) {
  fake_message *message;

  message = (fake_message *)self;
  return message->payload_handle;
}

static int fake_message_write_payload(lc_message *self, lc_sink *dst,
                                      size_t *written, lc_error *error) {
  static const char payload[] = "payload";
  fake_message *message;
  int rc;

  message = (fake_message *)self;
  message->write_payload_calls += 1;
  rc = dst->write(dst, payload, sizeof(payload) - 1U, error);
  if (!rc) {
    return LC_ERR_TRANSPORT;
  }
  if (written != NULL) {
    *written = sizeof(payload) - 1U;
  }
  message->payload_size = sizeof(payload) - 1U;
  return LC_OK;
}

static int fake_message_rewind_payload(lc_message *self, lc_error *error) {
  (void)self;
  (void)error;
  return LC_OK;
}

static lc_lease *fake_message_state(lc_message *self) {
  fake_message *message;

  message = (fake_message *)self;
  return message->state_handle;
}

static void fake_message_close(lc_message *self) {
  fake_message *message;

  message = (fake_message *)self;
  message->close_calls += 1;
}

static void test_client_wrapper_delegates_to_method_table(void **state) {
  fake_client client;
  fake_lease lease;
  lc_acquire_req req;
  lc_lease *out;
  lc_error error;
  int rc;

  (void)state;
  memset(&client, 0, sizeof(client));
  memset(&lease, 0, sizeof(lease));
  memset(&req, 0, sizeof(req));
  lc_error_init(&error);

  client.pub.acquire = fake_client_acquire;
  client.pub.close = fake_client_close;
  client.lease_to_return = &lease.pub;

  rc = lc_acquire(&client.pub, &req, &out, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(client.acquire_calls, 1);
  assert_ptr_equal(out, &lease.pub);

  lc_client_close(&client.pub);
  assert_int_equal(client.close_calls, 1);
  lc_error_cleanup(&error);
}

static void test_lease_wrapper_delegates_to_method_table(void **state) {
  fake_lease lease;
  lc_release_req req;
  lc_error error;
  int rc;

  (void)state;
  memset(&lease, 0, sizeof(lease));
  memset(&req, 0, sizeof(req));
  lc_error_init(&error);

  lease.pub.release = fake_lease_release;
  lease.pub.close = fake_lease_close;

  rc = lc_lease_release(&lease.pub, &req, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease.release_calls, 1);

  lc_lease_close(&lease.pub);
  assert_int_equal(lease.close_calls, 1);
  lc_error_cleanup(&error);
}

static void test_message_helpers_use_embedded_interface(void **state) {
  fake_message message;
  fake_lease lease;
  lc_sink *sink;
  lc_error error;
  const void *bytes;
  size_t length;
  size_t written;
  int rc;

  (void)state;
  memset(&message, 0, sizeof(message));
  memset(&lease, 0, sizeof(lease));
  sink = NULL;
  bytes = NULL;
  length = 0U;
  written = 0U;
  lc_error_init(&error);

  message.pub.payload_reader = fake_message_payload_reader;
  message.pub.write_payload = fake_message_write_payload;
  message.pub.rewind_payload = fake_message_rewind_payload;
  message.pub.state = fake_message_state;
  message.pub.close = fake_message_close;
  message.state_handle = &lease.pub;

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  assert_ptr_equal(lc_message_state(&message.pub), &lease.pub);
  assert_ptr_equal(lc_message_payload(&message.pub), NULL);
  rc = lc_message_write_payload(&message.pub, sink, &written, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(written, 7);
  assert_int_equal(message.write_payload_calls, 1);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, 7);
  assert_memory_equal(bytes, "payload", 7);
  rc = lc_message_rewind_payload(&message.pub, &error);
  assert_int_equal(rc, LC_OK);

  lc_message_close(&message.pub);
  assert_int_equal(message.close_calls, 1);
  lc_sink_close(sink);
  lc_error_cleanup(&error);
}

static void test_message_write_payload_handles_missing_payload(void **state) {
  lc_message_handle message;
  lc_sink *sink;
  lc_error error;
  const void *bytes;
  size_t length;
  size_t written;
  int rc;

  (void)state;
  memset(&message, 0, sizeof(message));
  sink = NULL;
  bytes = NULL;
  length = 99U;
  written = 99U;
  lc_error_init(&error);

  message.pub.payload_reader = lc_message_payload_reader_method;
  message.pub.write_payload = lc_message_write_payload_method;
  message.pub.rewind_payload = lc_message_rewind_payload_method;
  message.pub.state = lc_message_state_method;
  message.pub.close = lc_message_close_method;

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_message_write_payload(&message.pub, sink, &written, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(written, 0U);

  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, 0U);

  rc = lc_message_rewind_payload(&message.pub, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);

  lc_sink_close(sink);
  lc_error_cleanup(&error);
}

static void test_message_write_payload_rejects_null_sink(void **state) {
  fake_message message;
  lc_error error;
  int rc;

  (void)state;
  memset(&message, 0, sizeof(message));
  lc_error_init(&error);

  message.pub.write_payload = fake_message_write_payload;
  rc = lc_message_write_payload(&message.pub, NULL, NULL, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);

  lc_error_cleanup(&error);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_client_wrapper_delegates_to_method_table),
      cmocka_unit_test(test_lease_wrapper_delegates_to_method_table),
      cmocka_unit_test(test_message_helpers_use_embedded_interface),
      cmocka_unit_test(test_message_write_payload_handles_missing_payload),
      cmocka_unit_test(test_message_write_payload_rejects_null_sink),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}
