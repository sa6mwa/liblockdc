#ifndef LC_COLLECTION_MAINTENANCE_H
#define LC_COLLECTION_MAINTENANCE_H

#include "lc_pouch.h"

/* Existing root maintenance owns the cadence and lifetime. Discovery examines
 * catalog directories only; it never opens application state bodies. */
int lc_collection_maintenance_run(lc_pouch *pouch,
                                  const lc_pouch_maintenance_options *options,
                                  lc_pouch_maintenance_result *out,
                                  lc_error *error);
int lc_collection_maintenance_next(lc_pouch *pouch, const char *after,
                                   char **ns, char **cursor, lc_error *error);
/* Publish one coalesced namespace work reference before partition creation.
 * The caller holds namespace collection catalog authority. */
int lc_collection_maintenance_note(lc_pouch *pouch, const char *ns,
                                   lc_error *error);

#endif
