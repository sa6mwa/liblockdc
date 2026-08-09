# lockd memory queue watcher race

Status: upstream bug report candidate

Observed during the liblockdc `v0.13.0` release gate against `lockd:v0.8.1`.

## Summary

The `lockd` in-memory storage backend can panic while notifying queue watchers. The panic closes the active HTTP/Unix-socket request without a response, which surfaces in liblockdc as a transport error:

```text
Server returned nothing (no headers, no data)
```

This is a low-frequency race in the `lockd` memory backend, not evidence of a deterministic liblockdc client failure.

## Failing liblockdc test

The release gate failed in the e2e binary:

```text
lc_e2e_mem_consumer
```

Specific test:

```text
test_mem_uds_consumer_service_explicit_failure_nack
```

The failing assertion observed one unexpected consumer error event:

```text
unexpected consumer error event:
error_events=1
last_error_code=3
last_error_http_status=0
last_error_server_code=
last_error_message=Server returned nothing (no headers, no data)
start_events=2
stop_events=2
last_visibility_timeout_seconds=2
```

The consumer service retried after the transport error, which explains `start_events=2`. The test expects this explicit failure-nack flow to complete without consumer error events.

## Server-side evidence

The matching `lockd-mem` container log showed a Go panic at the same time:

```text
http: panic serving @: send on closed channel
```

Relevant stack frames from the log:

```text
pkt.systems/lockd/internal/storage/memory.(*memoryQueueSubscription).signal(...)
pkt.systems/lockd/internal/storage/memory/memory.go:759
pkt.systems/lockd/internal/storage/memory.(*Store).notifyQueue(...)
pkt.systems/lockd/internal/storage/memory/memory.go:682
pkt.systems/lockd/internal/storage/memory.(*Store).PutObject(...)
pkt.systems/lockd/internal/storage/memory/memory.go:581
pkt.systems/lockd/internal/queue.(*Service).SaveMessageDocument(...)
pkt.systems/lockd/internal/queue/service.go:922
pkt.systems/lockd/internal/queue.(*Service).IncrementAttempts(...)
pkt.systems/lockd/internal/queue/service.go:993
pkt.systems/lockd/internal/core.(*Service).prepareQueueDelivery(...)
pkt.systems/lockd/internal/core/queuedelivery.go:523
pkt.systems/lockd/internal/core.(*Service).consumeQueue(...)
pkt.systems/lockd/internal/core/queuedelivery.go:154
pkt.systems/lockd/internal/httpapi.(*Handler).handleQueueSubscribeInternal(...)
pkt.systems/lockd/internal/httpapi/handler_endpoints.go:3313
```

## Likely root cause

In `lockd` `v0.8.1`, `Store.notifyQueue()` snapshots subscriptions under `queueWatchMu`, releases the lock, then calls `sub.signal()`:

```go
func (s *Store) notifyQueue(queue string) {
	if !s.queueWatchEnabled {
		return
	}
	s.queueWatchMu.Lock()
	var subs []*memoryQueueSubscription
	for sub := range s.queueWatchers[queue] {
		subs = append(subs, sub)
	}
	s.queueWatchMu.Unlock()
	for _, sub := range subs {
		sub.signal()
	}
}
```

`memoryQueueSubscription.Close()` can concurrently remove and close the same subscription channel:

```go
func (s *memoryQueueSubscription) Close() error {
	if !atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		return nil
	}
	s.store.removeSubscription(s.queue, s)
	close(s.events)
	return nil
}
```

`signal()` checks `closed` before sending, but that check does not synchronize with channel close:

```go
func (s *memoryQueueSubscription) signal() {
	if atomic.LoadUint32(&s.closed) == 1 {
		return
	}
	select {
	case s.events <- struct{}{}:
	default:
	}
}
```

A bad interleaving is:

1. `notifyQueue()` copies a subscription into `subs`.
2. `signal()` observes `closed == 0`.
3. Another goroutine runs `Close()`, sets `closed = 1`, removes the subscription, and closes `events`.
4. `signal()` sends on the now-closed `events` channel.
5. Go panics with `send on closed channel`.

## Reproduction notes

The failure is timing-dependent. Follow-up stress did not reproduce it:

```text
CMOCKA_TEST_FILTER=test_mem_uds_consumer_service_explicit_failure_nack \
  build/e2e/tests/e2e/lc_e2e_mem_consumer
```

Result:

```text
50/50 isolated runs passed
```

Whole binary stress:

```text
build/e2e/tests/e2e/lc_e2e_mem_consumer
```

Result:

```text
10/10 full binary runs passed
140/140 test cases passed
```

This supports classifying the release-gate failure as a low-frequency race.

## Scope

This was observed only with the memory backend over Unix socket:

```text
--store mem:// --listen /run/lockd/lockd.sock --listen-proto unix
```

The disk and S3 consumer e2e suites passed before the memory consumer failure. That is consistent with this being specific to the in-memory queue watch implementation.

Local inspection found the same queue watcher pattern in the local `lockd` `v0.9.0` tag, so this may not be fixed upstream as of that tag.

## Suggested upstream fix direction

Make `memoryQueueSubscription.signal()` and `Close()` synchronize channel send/close ownership. Options include:

- avoid closing `events` while any notifier can still hold a copied subscription;
- protect send and close with a per-subscription mutex;
- replace channel close with cancellation signaling that does not race with nonblocking sends;
- keep `queueWatchMu` held across signal/close if that does not introduce deadlocks or unacceptable contention.

The important property is that no goroutine can send to `events` after it has been closed.
