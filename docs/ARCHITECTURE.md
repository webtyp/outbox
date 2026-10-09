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
