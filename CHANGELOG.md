# Changelog

All notable changes to this library are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

These changes are designed to ensure the continuous operation of the database or 
queue in the event of a failure or a leader change. when `maas-service` points to a 
node that has switched to follower mode, requests that previously failed immediately 
now wait for a new leader to appear.

### Added

- Rabbit CRUD calls (`GetOrCreateVhost`, `GetVhost`) are retried. Only the Kafka
  client retried before.
- A per-attempt timeout, 30s. It is taken from what is left of the call
  deadline, so it cannot overrun it, and a shorter caller deadline still wins.
  Neither level had a timeout before, so a hung agent held a call indefinitely.
- A backoff between failed watch requests: one second, doubling, capped at 30s,
  with +/-20% jitter and reset on success. The watch loop used to send the next
  request immediately, hammering the agent exactly while it was coming back up.
- `util.WithMaxTotalDuration` and `util.WithAttemptTimeout`, accepted by
  `kafka.NewClient` and `rabbit.NewClient`. Neither bound was reachable from a
  service before.
- `util.HttpError`, carrying the status of the response a call ended on, and
  `util.RetriesExhaustedError`, which says the call was repeated until its
  duration ran out and wraps the last failure. A caller could not tell a
  permanent `400` from an exhausted call before.

### Changed

- A call is bounded by its total duration, 60s, rather than by a count of 30
  attempts. Pauses grow from a second, are capped at a quarter of the total and
  carry +/-20% jitter, so callers that failed together do not return together.
  The pause used to be a flat second with no jitter.
- Which responses are retried. Every response used to be, `4xx` included; now
  `5xx` and `429` are, other `4xx` fail on the first attempt. `405` is the
  exception and is repeated only when the `reason` of the error envelope names a
  database that cannot be written, which is how maas-service reports a write
  against a demoted Patroni node. `401` in particular is no longer retried: the
  token provider refreshes on its own schedule, so a retry within the backoff
  re-sends the same token.
- The watch long-poll window is derived from the HTTP client timeout instead of
  being fixed at 60s, so maas-service answers before the client gives up.
- A failed call reports `maas-agent responded with status: ..., body: ...`. It
  used to be `response with error code reveived. Status: ..., body: ...`.
- Retries are built on failsafe-go, which is a new dependency.
- The resty client is expected to carry no retries of its own: this library
  retries, and resty retries on top would multiply the attempts.

### Fixed

- Tenant watch no longer holds the broadcaster lock across the resource fetch,
  which blocked the websocket read loop for the length of an HTTP call.
- A tenant watch round that gives up no longer leaves the broadcaster dead: the
  next `Watch` revives it.
- `Watch` no longer reports success when the round it joined was torn down
  before the registration took effect. The caller was left holding what looked
  like a live subscription.
- Cancelling a round no longer stops the watchers that belong to it.
- A watcher that stopped draining its queue no longer wedges the broadcaster.
- `Retry.Run` reports "failed after N attempts" instead of "after N retries".
  The count was always attempts.
