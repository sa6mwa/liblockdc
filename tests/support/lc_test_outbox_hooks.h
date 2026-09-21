#ifndef LC_TEST_OUTBOX_HOOKS_H
#define LC_TEST_OUTBOX_HOOKS_H

/* Test-only synchronization point after wait_command() has durably observed a
 * pending receipt. It keeps process E2E coordination deterministic without
 * exposing a production API or relying on timing sleeps. */
typedef void (*lc_test_outbox_hook_fn)(void *context);

extern lc_test_outbox_hook_fn
    lc_outbox_test_after_command_wait_pending_read_hook;
extern void *lc_outbox_test_after_command_wait_pending_read_context;

#endif
