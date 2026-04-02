# Benchmark Notes

This repo contains apples-to-apples framework comparisons, but some benches intentionally measure
different API contracts. Read the benchmark file comments before drawing conclusions.

## `compare_sync_tell_current`

- `actor_framework_sync` is the current public `SyncActorRef::tell(...)` path.
- `actor_framework_sync_snapshot` is a lower-level mailbox baseline, not the preferred public API.
- Payloads are randomized and every sink actor updates a checksum so repeated constant tells are
  not the dominant benchmark signal.

Important interpretation note:

- `AF_CMP_SYNC_MAILBOX_CAP` matters a lot.
- With a normal bounded capacity, results include admission-pressure and retry/backoff behavior.
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

## `compare_async_tell_current`

- Payloads are randomized and consumed into a checksum across all frameworks.
- This bench is meant to compare raw async tell throughput, not sync->async delegated completion.
