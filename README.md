# `webtyp.com/outbox`

A durable, ordered queue of pending mutations with retry, rejection, and coalescing.

Like the "Outbox" folder of an email client: it stores what you wrote and has not been sent yet.

It runs in the browser (compiles to TinyGo WASM, over `webtyp/indexdb`) and in tests (over `webtyp.com/storage/mem`), always through `*orm.DB`. The browser environment creates the store from the model directly; backend environments can create it with `migrate.Migrate`.

## Warning
**Deliver blocks until every due mutation was sent; never call it from a JavaScript event callback in WASM — run it in its own goroutine.**

## One-tab rule
Only one writer (one tab) per database at a time is supported. Ensure only a single tab is active for this.

## Example
```go
box, _ := outbox.New(orm.New(conn), ids, time.Now)
id, _ := box.Enqueue(outbox.Entry{Op: "clinical_encounter.save_draft", Payload: b, Coalesce: visitID})
go func() { report, err := box.Deliver(sender) /* … */ }()
```
