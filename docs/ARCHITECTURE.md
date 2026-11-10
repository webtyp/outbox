# Architecture

## I want X → use Y
- I want to queue a mutation → `Enqueue`
- I want to send all pending mutations to the server → `Deliver`
- I want to see how many mutations are waiting → `Pending`
- I want to resolve rejected mutations → `Dismiss`
- I want to retry mutations now instead of waiting → `RetryNow`
- I want to send delivered mutations again (e.g. after a server failover) → `Redeliver`
- I want to clean up successfully sent mutations → `Prune`

## Metaphor
The system behaves just like an email client's "Outbox". Changes are staged and ordered in the Outbox. They wait until the client regains connectivity to send them to the server sequentially, reporting either delivery success, rejection (for good), or retry logic on transient errors.

## Retry delay
When the server answers `Retry`, the mutation waits 1 s after the first failure and doubles after each further failure up to 60 s. The calculation lives in `webtyp.com/backoff` (one library for every retry delay in the framework). See https://github.com/webtyp/backoff/blob/main/README.md.
