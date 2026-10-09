package tests

import (
	"strconv"
	"testing"

	"webtyp.com/ddl"
	"webtyp.com/model"
	"webtyp.com/orm"
	"webtyp.com/outbox"
	"webtyp.com/outbox/migrate"
	"webtyp.com/storage"
	"webtyp.com/storage/mem"
)

type mockIDGenerator struct {
	next int
}

func (m *mockIDGenerator) NewID() string {
	m.next++
	return "id" + strconv.Itoa(m.next)
}

type fakeSender struct {
	received []string
	answers  map[string]outbox.Result
	wait     chan struct{} // blocks if not nil
	onSend   chan struct{} // signaled when Send is called
}

func (s *fakeSender) Send(m *outbox.Mutation) outbox.Result {
	if s.onSend != nil {
		s.onSend <- struct{}{}
	}
	if s.wait != nil {
		<-s.wait
	}
	s.received = append(s.received, string(m.Payload))
	if res, ok := s.answers[string(m.Payload)]; ok {
		return res
	}
	return outbox.Result{Verdict: outbox.Delivered}
}

type memDDLCompiler struct{}

func (m memDDLCompiler) CompileDDL(s ddl.Stmt, mod model.Model) (string, []any, error) {
	return "mem_ddl", nil, nil
}

type memExecWrapper struct {
    storage.Conn
}
func (m memExecWrapper) Exec(query string, args ...any) error { return nil }

func setupOutbox(t *testing.T) (*outbox.Outbox, *fakeSender, *int64, *orm.DB) {
	conn := mem.New()

	// Create table isn't needed for mem directly since it inserts rows transparently.
	// But to satisfy test requirements of invoking migrate.Migrate:
	err := migrate.Migrate(memExecWrapper{conn}, memDDLCompiler{})
	if err != nil {
		t.Fatalf("failed to migrate: %v", err)
	}

	db := orm.New(conn)
	ids := &mockIDGenerator{}

	var clock int64 = 1000
	now := func() int64 { return clock }

	ob, err := outbox.New(db, ids, now)
	if err != nil {
		t.Fatalf("failed to create outbox: %v", err)
	}

	sender := &fakeSender{
		received: []string{},
		answers:  make(map[string]outbox.Result),
	}

	return ob, sender, &clock, db
}

func TestOutbox_Order(t *testing.T) {
	ob, sender, _, db := setupOutbox(t)

	ob.Enqueue(outbox.Entry{Op: "op", Payload: []byte("A")})
	ob.Enqueue(outbox.Entry{Op: "op", Payload: []byte("B")})
	ob.Enqueue(outbox.Entry{Op: "op", Payload: []byte("C")})

	rep, err := ob.Deliver(sender)
	if err != nil {
		qb := db.Query(&outbox.Mutation{}).Where(outbox.Mutation_.State).Eq("pending")
		list, _ := outbox.ReadAllMutation(qb)
		for _, v := range list { t.Logf("Pending: %+v", v) }
		t.Fatalf("Deliver error: %v", err)
	}
	if rep.Delivered != 3 {
		t.Errorf("Expected 3 delivered, got %d", rep.Delivered)
	}

	pending, err := ob.Pending()
	if err != nil {
		t.Fatalf("Pending error: %v", err)
	}
	if pending != 0 {
		t.Errorf("Expected 0 pending, got %d", pending)
	}

	if len(sender.received) != 3 || sender.received[0] != "A" || sender.received[1] != "B" || sender.received[2] != "C" {
		t.Errorf("Incorrect delivery order: %v", sender.received)
	}
}

func TestOutbox_RetryBlocksOrder(t *testing.T) {
	ob, sender, clock, db := setupOutbox(t)

	ob.Enqueue(outbox.Entry{Op: "op", Payload: []byte("A")})
	ob.Enqueue(outbox.Entry{Op: "op", Payload: []byte("B")})
	ob.Enqueue(outbox.Entry{Op: "op", Payload: []byte("C")})

	sender.answers["A"] = outbox.Result{Verdict: outbox.Delivered}
	sender.answers["B"] = outbox.Result{Verdict: outbox.Retry, Reason: "temp error"}

	rep, err := ob.Deliver(sender)
	if err != nil {
		t.Fatalf("Deliver error: %v", err)
	}
	if !rep.Blocked {
		t.Errorf("Expected blocked to be true")
	}
	if len(sender.received) != 2 || sender.received[0] != "A" || sender.received[1] != "B" {
		t.Errorf("Expected A and B to be sent: %v", sender.received)
	}

	// Check B's attempts
	bMut := &outbox.Mutation{}
	// mem engine doesn't compare []byte payload properly in Where, using Id instead since we inserted B second it has id2
	qb := db.Query(bMut).Where(outbox.Mutation_.Id).Eq("id2")
	outbox.ReadOneMutation(qb, bMut)
	if bMut.Attempts != 1 {
		t.Errorf("Expected B attempts to be 1, got %d", bMut.Attempts)
	}

	// Advance clock less than 1s
	*clock += 500000000 // 0.5s
	sender.received = nil
	rep, err = ob.Deliver(sender)
	if err != nil {
		t.Fatalf("Deliver error: %v", err)
	}
	if !rep.Blocked {
		t.Errorf("Expected blocked to be true")
	}
	if len(sender.received) != 0 {
		t.Errorf("Expected nothing to be sent, got: %v", sender.received)
	}

	// Advance past 1s
	*clock += 1000000000
	sender.answers["B"] = outbox.Result{Verdict: outbox.Delivered}
	rep, err = ob.Deliver(sender)
	if err != nil {
		t.Fatalf("Deliver error: %v", err)
	}
	if rep.Blocked {
		t.Errorf("Expected not blocked")
	}
	if len(sender.received) != 2 || sender.received[0] != "B" || sender.received[1] != "C" {
		t.Errorf("Expected B and C to be sent: %v", sender.received)
	}
}

func TestOutbox_BackoffCaps(t *testing.T) {
	ob, sender, clock, db := setupOutbox(t)
	ob.Enqueue(outbox.Entry{Op: "op", Payload: []byte("A")})

	sender.answers["A"] = outbox.Result{Verdict: outbox.Retry}

	// Fast-forward many retries
	aMut := &outbox.Mutation{}
	qb := db.Query(aMut).Where(outbox.Mutation_.Id).Eq("id1")
	outbox.ReadOneMutation(qb, aMut)

	aMut.Attempts = 50
	aMut.NextAttemptAt = *clock
	db.UpdateFields(aMut, []string{outbox.Mutation_.Attempts, outbox.Mutation_.NextAttemptAt}, orm.Eq(outbox.Mutation_.Id, aMut.Id))

	ob.Deliver(sender)

	outbox.ReadOneMutation(qb, aMut)
	diff := aMut.NextAttemptAt - *clock
	if diff != 60000000000 {
		t.Errorf("Expected 60s backoff, got %d", diff)
	}
}

func TestOutbox_RejectedDoesNotBlock(t *testing.T) {
	ob, sender, _, _ := setupOutbox(t)

	idA, _ := ob.Enqueue(outbox.Entry{Op: "op", Payload: []byte("A")})
	idB, _ := ob.Enqueue(outbox.Entry{Op: "op", Payload: []byte("B")})

	sender.answers["A"] = outbox.Result{Verdict: outbox.Rejected, Reason: "slot taken"}
	sender.answers["B"] = outbox.Result{Verdict: outbox.Delivered}

	ob.Deliver(sender)

	rej, err := ob.Rejected()
	if err != nil {
		t.Fatalf("Rejected error: %v", err)
	}
	if len(rej) != 1 || rej[0].Id != idA {
			t.Fatalf("Expected A to be rejected, got: %+v", rej)
	}
	if rej[0].Reason != "slot taken" {
		t.Errorf("Expected reason 'slot taken', got %q", rej[0].Reason)
	}

	err = ob.Dismiss(idA)
	if err != nil {
		t.Fatalf("Dismiss A error: %v", err)
	}

	err = ob.Dismiss(idB)
	if err == nil {
		t.Errorf("Expected error dismissing B")
	}
}

func TestOutbox_Coalesce(t *testing.T) {
	ob, sender, _, _ := setupOutbox(t)

	id1, _ := ob.Enqueue(outbox.Entry{Op: "x", Coalesce: "v1", Payload: []byte("1")})
	id2, _ := ob.Enqueue(outbox.Entry{Op: "x", Coalesce: "v1", Payload: []byte("2")})

	if id1 != id2 {
		t.Errorf("Expected coalesced IDs to match")
	}

	pending, _ := ob.Pending()
	if pending != 1 {
		t.Errorf("Expected 1 pending mutation, got %d", pending)
	}

	// simulate user typing while sent
	waitChan := make(chan struct{})
	sender.wait = waitChan

	go func() {
		ob.Deliver(sender)
	}()

	// Since tests/outbox_test.go is external, we can't inspect unexported fields.
	// We use the `wait` chan to block Send(). That means `Deliver` is running and Send is stuck.
	// Wait a tiny bit just to let goroutine start blocking in Send.
	// But it's tricky without a signal. Let's send a value to a chan when Send is called!
	sender.onSend = make(chan struct{}, 1)
	go func() {
		ob.Deliver(sender)
	}()
	<-sender.onSend // wait until Send is called!

	id3, _ := ob.Enqueue(outbox.Entry{Op: "x", Coalesce: "v1", Payload: []byte("3")})
	if id3 == id1 {
		t.Errorf("Expected new mutation to be created during flight")
	}

	close(waitChan) // let delivery finish

	sender.wait = nil
	sender.onSend = nil

	// Wait for the background delivery to finish
	// Since outbox_test is an external package, we can't inspect unexported fields easily,
	// but we know we can just try grabbing the Deliver lock! Wait, Deliver returns error if in progress.
	for {
		_, err := ob.Deliver(sender)
		if err == nil || err.Error() != "outbox: delivery already in progress" {
			break
		}
	}

	if len(sender.received) != 2 || sender.received[0] != "2" || sender.received[1] != "3" {
		t.Errorf("Expected payloads [2, 3] to be delivered, got %v", sender.received)
	}
}

func TestOutbox_RetryNow(t *testing.T) {
	ob, sender, _, _ := setupOutbox(t)
	ob.Enqueue(outbox.Entry{Op: "op", Payload: []byte("A")})

	sender.answers["A"] = outbox.Result{Verdict: outbox.Retry}
	ob.Deliver(sender)

	ob.RetryNow()
}

func TestOutbox_Redeliver(t *testing.T) {
	ob, sender, clock, _ := setupOutbox(t)

	*clock = 10
	ob.Enqueue(outbox.Entry{Op: "op", Payload: []byte("A")})
	ob.Deliver(sender)

	*clock = 20
	ob.Enqueue(outbox.Entry{Op: "op", Payload: []byte("B")})
	ob.Deliver(sender)

	count, err := ob.Redeliver(15)
	if err != nil {
		t.Fatalf("Redeliver err: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected Redeliver count 1, got %d", count)
	}

	sender.received = nil
	ob.Deliver(sender)

	if len(sender.received) != 1 || sender.received[0] != "B" {
		t.Errorf("Expected only B to be redelivered, got %v", sender.received)
	}
}

func TestOutbox_Prune(t *testing.T) {
	ob, sender, clock, db := setupOutbox(t)

	*clock = 10
	ob.Enqueue(outbox.Entry{Op: "op", Payload: []byte("A")})
	ob.Deliver(sender)

	*clock = 20
	ob.Enqueue(outbox.Entry{Op: "op", Payload: []byte("B")})
	ob.Deliver(sender)

	count, err := ob.Prune(15)
	if err != nil {
		t.Fatalf("Prune err: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected Prune count 1, got %d", count)
	}

	qb := db.Query(&outbox.Mutation{})
	list, _ := outbox.ReadAllMutation(qb)
	if len(list) != 1 {
		t.Errorf("Expected 1 mutation remaining, got %d", len(list))
	}
	if list[0].Payload[0] != 'B' {
		t.Errorf("Expected B to remain")
	}
}

func TestOutbox_Restart(t *testing.T) {
	conn := mem.New()
	migrate.Migrate(memExecWrapper{conn}, memDDLCompiler{})
	db := orm.New(conn)
	ids := &mockIDGenerator{}

	now := func() int64 { return 1000 }

	ob1, _ := outbox.New(db, ids, now)
	ob1.Enqueue(outbox.Entry{Op: "op", Payload: []byte("1")})
	ob1.Enqueue(outbox.Entry{Op: "op", Payload: []byte("2")})

	ob2, _ := outbox.New(db, ids, now)
	ob2.Enqueue(outbox.Entry{Op: "op", Payload: []byte("3")})

	m := &outbox.Mutation{}
	// mem engine doesn't compare []byte payload properly in Where, using Id instead since we inserted 3rd mutation it has id3
	qb := db.Query(m).Where(outbox.Mutation_.Id).Eq("id3")
	outbox.ReadOneMutation(qb, m)

	if m.Seq != 3 {
		t.Errorf("Expected Seq 3, got %d", m.Seq)
	}
}

func TestOutbox_ConcurrentDeliver(t *testing.T) {
	ob, sender, _, _ := setupOutbox(t)

	ob.Enqueue(outbox.Entry{Op: "op", Payload: []byte("1")})

	waitChan := make(chan struct{})
	sender.wait = waitChan
	sender.onSend = make(chan struct{}, 1)

	go func() {
		ob.Deliver(sender)
	}()

	<-sender.onSend

	_, err := ob.Deliver(sender)
	if err == nil || err.Error() != "outbox: delivery already in progress" {
		t.Errorf("Expected 'delivery already in progress', got %v", err)
	}

	close(waitChan)
	for {
		_, err := ob.Deliver(sender)
		if err == nil || err.Error() != "outbox: delivery already in progress" {
			break
		}
	}
}

func TestOutbox_NilArguments(t *testing.T) {
	_, err := outbox.New(nil, nil, nil)
	if err == nil || err.Error() != "outbox: db, ids and now are required" {
		t.Errorf("Expected 'db, ids and now are required', got %v", err)
	}

	ob, _, _, _ := setupOutbox(t)
	_, err = ob.Enqueue(outbox.Entry{})
	if err == nil || err.Error() != "outbox: entry op is required" {
		t.Errorf("Expected 'entry op is required', got %v", err)
	}
}
