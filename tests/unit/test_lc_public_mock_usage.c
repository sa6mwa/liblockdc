#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>

#include <cmocka.h>

#include "lc/lc.h"
#include "mock_lc_public.h"

static int public_mock_managed_terminal_action(lc_message *message,
                                               int handler_rc,
                                               int already_terminal,
                                               lc_error *error) {
  lc_nack_req nack_req;

  if (handler_rc == LC_OK) {
    if (!already_terminal) {
      return lc_message_ack(message, error);
    }
    return LC_OK;
  }
  if (!already_terminal) {
    lc_nack_req_init(&nack_req);
    nack_req.intent = LC_NACK_INTENT_FAILURE;
    nack_req.delay_seconds = 0L;
    return lc_message_nack(message, &nack_req, error);
  }
  return handler_rc;
}

static void test_public_mocks_support_happy_path_flow(void **state) {
  lc_public_mock_client client;
  lc_public_mock_lease lease;
  lc_public_mock_message message;
  lc_acquire_req acquire_req;
  lc_release_req release_req;
  lc_dequeue_req dequeue_req;
  lc_nack_req nack_req;
  lc_lease *lease_out;
  lc_message *message_out;
  lc_error error;
  int rc;

  (void)state;
  lc_public_mock_client_init(&client);
  lc_public_mock_lease_init(&lease);
  lc_public_mock_message_init(&message);
  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  lc_dequeue_req_init(&dequeue_req);
  lc_nack_req_init(&nack_req);
  lc_error_init(&error);

  client.lease_to_return = &lease.pub;
  client.message_to_return = &message.pub;
  lease_out = NULL;
  message_out = NULL;

  rc = lc_acquire(&client.pub, &acquire_req, &lease_out, &error);
  assert_int_equal(rc, LC_OK);
  assert_ptr_equal(lease_out, &lease.pub);

  rc = lc_lease_release(lease_out, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease.release_call.count, 1);

  rc = lc_dequeue(&client.pub, &dequeue_req, &message_out, &error);
  assert_int_equal(rc, LC_OK);
  assert_ptr_equal(message_out, &message.pub);

  rc = lc_message_nack(message_out, &nack_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(message.nack_call.count, 1);

  lc_error_cleanup(&error);
}

static void
test_public_mocks_model_managed_consumer_terminal_actions(void **state) {
  lc_public_mock_message message;
  lc_nack_req nack_req;
  lc_error error;
  int rc;

  (void)state;
  lc_public_mock_message_init(&message);
  lc_nack_req_init(&nack_req);
  lc_error_init(&error);

  rc = public_mock_managed_terminal_action(&message.pub, LC_OK, 0, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(message.ack_call.count, 1);
  assert_int_equal(message.nack_call.count, 0);

  lc_public_mock_message_init(&message);
  rc = lc_message_nack(&message.pub, &nack_req, &error);
  assert_int_equal(rc, LC_OK);
  rc = public_mock_managed_terminal_action(&message.pub, LC_ERR_TRANSPORT, 1,
                                           &error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_int_equal(message.ack_call.count, 0);
  assert_int_equal(message.nack_call.count, 1);

  lc_public_mock_message_init(&message);
  nack_req.intent = LC_NACK_INTENT_DEFER;
  rc = lc_message_nack(&message.pub, &nack_req, &error);
  assert_int_equal(rc, LC_OK);
  rc = public_mock_managed_terminal_action(&message.pub, LC_ERR_TRANSPORT, 1,
                                           &error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_int_equal(message.ack_call.count, 0);
  assert_int_equal(message.nack_call.count, 1);

  lc_error_cleanup(&error);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_public_mocks_support_happy_path_flow),
      cmocka_unit_test(
          test_public_mocks_model_managed_consumer_terminal_actions),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}
