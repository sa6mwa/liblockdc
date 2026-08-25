# lockd implicit-XA workflow blocker

## Summary

liblockdc's workflow API starts a transaction by acquiring its first durable
record without a caller-provided XID. lockd mints that XID. Every later domain,
inbox, command, and outbox acquire carries the minted XID, and the workflow
then releases every participant to make the terminal decision.

Pouch implements this correctly: a multi-participant implicit transaction does
not publish any participant when the first release arrives. It publishes all
participants only once every enrolled participant has voted to commit, and it
rolls all of them back when a participant votes rollback or expires.

The pinned remote lockd environment currently diverges. After a first acquire
without a supplied XID, adding a later participant with the returned XID does
not reliably enroll the first lease in the same implicit-XA decision. Releasing
that first lease can publish its staged state before the later participant has
committed. A crash or failed release of the second participant can therefore
leave an outbox/command receipt visible while the domain mutation is absent.

That violates the atomicity required for transactional messaging.

## Minimal reproduction

1. Acquire key `outbox` without a transaction ID and stage an outbox record.
2. Read the minted XID from that lease.
3. Acquire key `domain` with that XID and stage the business state.
4. Release `outbox` with commit.
5. Stop before committing `domain`, or make its release fail.
6. Reopen and read both keys.

Expected: neither key is publicly visible until both participants commit; after
the interrupted flow both are absent/rolled back.

Observed against the affected remote lockd build: `outbox` can be public while
`domain` remains uncommitted.

## Required lockd behavior

For an implicitly minted XID, the server must preserve the first participant's
enrollment when subsequent acquires use that XID. A commit vote from any single
participant is a prepare vote, not a final decision, while another enrolled
participant remains undecided. A rollback vote or participant expiry must
decide rollback for the whole transaction. Explicit caller-supplied XA records
must retain their existing decision behavior and must not be rewritten as
implicit pending records.

## liblockdc posture

liblockdc intentionally has no remote-endpoint guard or Pouch-specific public
API. The workflow logic uses the ordinary client contract and will exercise the
same composition against remote lockd once this server behavior is repaired.
This branch proves exclusive and shared-writer Pouch behavior; remote command
receipt E2E is deferred pending the lockd fix.
