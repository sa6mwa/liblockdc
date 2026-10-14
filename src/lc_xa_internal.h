#ifndef LC_XA_INTERNAL_H
#define LC_XA_INTERNAL_H

#include "lc_api_internal.h"

/* Managed coordination uses the existing .txns decision/vote engine. .xa owner
 * leases supply per-transaction fencing and enrollment sealing, not decisions.
 * These calls run on the coordinator's serialized durability worker. */
typedef struct lc_xa_snapshot {
  char txn_id[LC_XID_STRING_SIZE];
  int decision; /* 0 preparing, 1 commit, 2 abort. */
  int sealed;
  int claim_acquired; /* This claim installed ownership; existing live claims
                       * and completed-history lookup leave this false. */
  lc_unix_seconds completed_at_unix;
  lc_tc_term term;
  size_t branch_count;
  lc_xa_branch_record *branches;
} lc_xa_snapshot;

int lc_xa_store_open_owned(lc_client *client, const char *owner_id,
                           lc_xa_store **out, lc_error *error);
int lc_xa_store_claim_blocking(lc_xa_store *store, const char *txn_id,
                               long ttl_seconds, int recover,
                               lc_xa_snapshot *out, lc_error *error);
int lc_xa_store_renew_blocking(lc_xa_store *store, const char *txn_id,
                               long ttl_seconds, lc_error *error);
int lc_xa_store_release_blocking(lc_xa_store *store, const char *txn_id,
                                 lc_error *error);
int lc_xa_store_snapshot_blocking(lc_xa_store *store, const char *txn_id,
                                  lc_xa_snapshot *out, lc_error *error);
int lc_xa_store_seal_blocking(lc_xa_store *store, const char *txn_id,
                              lc_error *error);
int lc_xa_store_decide_blocking(lc_xa_store *store, const char *txn_id,
                                int commit, lc_error *error);
int lc_xa_store_prepare_pouch_blocking(lc_xa_store *store, const char *txn_id,
                                       lc_error *error);
void lc_xa_snapshot_cleanup(lc_xa_snapshot *snapshot);
/* Core facade composition. The managed transaction owns a facade reference;
 * the facade's backlink is weak and cleared before that reference is dropped.
 */
int lc_xa_transaction_attach_outbox(lc_xa_transaction *transaction,
                                    lc_outbox_transaction *outbox,
                                    lc_client *client, lc_error *error);
int lc_outbox_xa_prepare(lc_outbox_transaction *outbox, int commit,
                         lc_error *error);
void lc_outbox_xa_seal(lc_outbox_transaction *outbox);
void lc_outbox_xa_unprepare(lc_outbox_transaction *outbox);
void lc_outbox_xa_applied(lc_outbox_transaction *outbox, int decision);
void lc_outbox_xa_detach(lc_outbox_transaction *outbox);
void lc_outbox_pouch_notify(lc_client *client, const char *ns, const char *key);

/* Backend entry points take the immutable owner nonce captured by the store.
 * NULL is the raw/manual journal; it cannot advance a managed transaction. */
int lc_pouch_client_xa_claim(lc_client *client, const char *owner_id,
                             const char *txn_id, long ttl_seconds, int recover,
                             lc_xa_snapshot *out, lc_error *error);
int lc_pouch_client_xa_owner_update(lc_client *client, const char *owner_id,
                                    const char *txn_id, long ttl_seconds,
                                    int seal, int release, lc_error *error);
int lc_pouch_client_xa_snapshot(lc_client *client, const char *txn_id,
                                lc_xa_snapshot *out, lc_error *error);
int lc_pouch_client_xa_decide(lc_client *client, const char *owner_id,
                              const char *txn_id, int commit, lc_error *error);
int lc_pouch_client_xa_prepare_pouch(lc_client *client, const char *owner_id,
                                     const char *txn_id, lc_error *error);
int lc_pouch_client_xa_retire(lc_client *client, long retention_seconds,
                              size_t max_records, size_t *retired_count,
                              lc_error *error);
#endif
