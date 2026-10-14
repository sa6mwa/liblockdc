#ifndef LC_POUCH_XA_H
#define LC_POUCH_XA_H

#include "lc_pouch.h"

/* Private storage adapter for external participants in the existing Pouch TC.
 * The .txns record alone decides the transaction. These .xa records describe
 * branches and record their progress; they never make a second decision.
 */
#define LC_POUCH_XA_NAMESPACE ".xa"
#define LC_POUCH_XA_KEY_SIZE (7U + 2U * LC_XID_STRING_SIZE)

int lc_pouch_xa_branch_key(const lc_xa_branch_record *branch, char *out,
                           size_t capacity, lc_error *error);
int lc_pouch_xa_branch_read(lc_pouch *pouch, const char *key,
                            lc_xa_branch_record *out, lc_error *error);
int lc_pouch_xa_branch_write(lc_pouch *pouch, const lc_xa_branch_record *branch,
                             lc_error *error);
int lc_pouch_xa_participant(const lc_txn_participant *participant);
int lc_pouch_xa_has_participants(const lc_txn_decision_req *request);
int lc_pouch_xa_application_gate(lc_pouch *pouch,
                                 const lc_txn_decision_req *request,
                                 const char *decision, lc_error *error);

/* Called by the resource journal only after the driver obtains and validates
 * native identity. Enrollment precedes prepare; phase intent precedes sending.
 * Completion evidence may be recorded only in the durable decision's direction.
 */
int lc_pouch_client_xa_enroll(lc_client *client,
                              const lc_xa_branch_record *branch,
                              const char *owner_id, lc_error *error);
int lc_pouch_client_xa_progress(lc_client *client,
                                const lc_xa_branch_record *branch,
                                const char *owner_id, lc_error *error);

int lc_pouch_client_xa_prepared(lc_client *client, const char *txn_id,
                                const char *key, unsigned char phase,
                                int *prepared, lc_error *error);

#endif
