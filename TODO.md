# liblockdc TODO

This file tracks the real lockd HTTP surface from `../lockd/internal/httpapi/handler.go` and the Go client behavior from `../lockd/client/`.

## Foundation

- [x] Map the registered server routes from the lockd HTTP handler.
- [x] Confirm the client bundle format used by lockd-generated `client.pem` files.
- [x] Establish an initial CMake build with both static and shared library targets.
- [x] Add CMake-managed third-party dependency builds for OpenSSL, nghttp2, and libcurl.
- [x] Add YAJL as the streaming JSON generator/parser dependency.
- [x] Define the initial public C API around config/request/response structs.
- [x] Add client-level allocator hooks for new stream-first APIs.
- [x] Implement the first transport slice with mTLS bundle loading, HTTP/1.1 + HTTP/2 preference, raw state bodies, multipart queue payloads, and endpoint failover for `node_passive`.
- [x] Implement `POST /v1/acquire`.
- [x] Implement `GET /v1/get` with public-read support.
- [x] Implement `POST /v1/update`.
- [x] Implement `POST /v1/mutate`.
- [x] Implement `POST /v1/metadata`.
- [x] Implement `POST /v1/remove`.
- [x] Implement `GET /v1/describe`.
- [x] Implement `POST /v1/keepalive`.
- [x] Implement `POST /v1/release`.
- [x] Implement `POST /v1/query` with raw selector JSON input.
- [x] Add stream-first `query` API that writes response bytes incrementally.
- [x] Implement `POST /v1/queue/enqueue`.
- [x] Implement `POST /v1/queue/dequeue`.
- [x] Implement `POST /v1/queue/dequeueWithState`.
- [x] Implement `POST /v1/queue/stats`.
- [x] Implement `POST /v1/queue/ack`.
- [x] Implement `POST /v1/queue/nack`.
- [x] Implement `POST /v1/queue/extend`.
- [x] Add `examples/` programs to exercise the public API shape.
- [x] Add a root-level docker-compose e2e environment with disk, S3/MinIO, and mem-backed lockd instances.
- [x] Add initial unit-test targets for stream helpers and public handle-contract wrappers.
- [ ] Add standard unit-test targets for JSON helpers, bundle parsing, and response decoding.
- [ ] Finish migrating the remaining convenience-only buffered paths onto the allocator-aware/stream-first plumbing.
- [ ] Add an integration test harness against a containerized lockd instance.
- [ ] Add install/export/package rules for downstream consumers.

## API coverage

- [x] `POST /v1/acquire`
- [x] `POST /v1/keepalive`
- [x] `POST /v1/release`
- [x] `POST /v1/get`
- [x] `POST /v1/attachments`
- [x] `POST /v1/attachment`
- [x] `POST /v1/query`
- [x] `POST /v1/mutate`
- [x] `POST /v1/update`
- [x] `POST /v1/metadata`
- [x] `POST /v1/remove`
- [x] `GET /v1/describe`
- [x] `POST /v1/index/flush`
- [x] `GET /v1/namespace`
- [x] `PUT /v1/namespace`
- [x] `POST /v1/queue/enqueue`
- [x] `POST /v1/queue/stats`
- [x] `POST /v1/queue/dequeue`
- [x] `POST /v1/queue/dequeueWithState`
- [x] `POST /v1/queue/watch`
- [x] `POST /v1/queue/subscribe`
- [x] `POST /v1/queue/subscribeWithState`
- [x] `POST /v1/queue/ack`
- [x] `POST /v1/queue/nack`
- [x] `POST /v1/queue/extend`
- [x] `POST /v1/txn/replay`
- [x] `POST /v1/txn/decide`
- [x] `POST /v1/txn/commit`
- [x] `POST /v1/txn/rollback`
- [x] `POST /v1/tc/lease/acquire`
- [x] `POST /v1/tc/lease/renew`
- [x] `POST /v1/tc/lease/release`
- [x] `GET /v1/tc/leader`
- [x] `POST /v1/tc/cluster/announce`
- [x] `POST /v1/tc/cluster/leave`
- [x] `GET /v1/tc/cluster/list`
- [x] `POST /v1/tc/rm/register`
- [x] `POST /v1/tc/rm/unregister`
- [x] `GET /v1/tc/rm/list`

## Next pass recommendation

- [ ] Polish attachment DX around the new `lc_lease` object model.
- [ ] Add query result helpers beyond raw JSON and cover document-return mode explicitly.
- [ ] Add stream-first dequeue payload APIs.
- [ ] Replace the remaining ad hoc JSON handling in older client paths with YAJL-backed helpers.
- [ ] Add client-driven e2e tests against the root docker-compose lockd environment, including the UDS-backed mem instance.
