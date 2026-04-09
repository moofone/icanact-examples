# Benchmark Notes

This repo contains apples-to-apples framework comparisons, but some benches intentionally measure
different API contracts. Read the benchmark file comments before drawing conclusions.

## `compare_sync_tell_current`

- `actor_framework_sync` is the current public `SyncActorRef::tell(...)` path.
- With `AF_CMP_TELL_PRODUCERS=8`, the bench also exposes `actor_framework_sync_unsharded`
  (`tell_shards=1`) and `actor_framework_sync_sharded_8` (`tell_shards=8`) so the contention
  tradeoff is explicit.
- Payloads are randomized and every sink actor updates a checksum so repeated constant tells are
  not the dominant benchmark signal.

Important interpretation note:

- `AF_CMP_SYNC_MAILBOX_CAP` matters a lot.
- With a normal bounded capacity, results include admission-pressure and retry/backoff behavior.
- The benchmark now defaults `AF_CMP_SYNC_MAILBOX_CAP` to at least `1000000` so the default run is
  much closer to Actix's effectively unbounded sync queue.
- With a very large capacity like `AF_CMP_SYNC_MAILBOX_CAP=1000000`, the benchmark becomes much
  closer to a near-unbounded steady-state send-cost comparison.

Actix note:

- The current `actix_sync` lane uses `do_send`, so it should be read as a convenience-API
  comparison, not a strict bounded-admission equivalent to the actor-framework bounded tell path.

## `compare_sync_async_ask_current`

This benchmark is intended to compare sync-origin delegated completion into an async worker.

- `actor_framework_sync_to_async_tell_result_to` is the intended public actor-framework path.
- It now routes completion directly into the target sync mailbox, which is the default and fastest
  supported delegated-completion route in the core crate.
- `actix_sync_to_async_reply_to_sync` is the closest Actix equivalent in this repo.

Apples-to-apples note:

- The comparison intentionally does **not** use `Addr::send(...).await` on the Actix side, because
  that changes the contract to caller-owned waiting instead of tell-shaped delegated completion.
- `AF_CMP_ASYNC_MAILBOX_CAP` now defaults to at least `1000000` so the async worker ingress is
  close to effectively unbounded by default.

## `compare_async_tell_current`

- Payloads are randomized and consumed into a checksum across all frameworks.
- This bench is meant to compare raw async tell throughput, not sync->async delegated completion.

## `spsc_sync_actor_pair`

- ICA-only end-to-end SPSC channel benchmark for one sync sender actor feeding one sync receiver
  actor.
- Startup is outside the timed window; the run starts on one kickoff tell to the sender actor.
- Capacity defaults to `max(requested_cap, msgs)` so the result is about channel transport and
  receiver processing rather than admission pressure.

## `spsc_async_to_sync_actor`

- ICA-only end-to-end SPSC channel benchmark for one async sender actor feeding one sync receiver
  actor.
- Startup is outside the timed window; the run starts on one kickoff tell to the async sender
  actor.
- Capacity defaults to `max(requested_cap, msgs)` for the same reason as the sync-pair bench.
