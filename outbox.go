package outbox

import (
	"sync"

	"webtyp.com/model"
	"webtyp.com/orm"
)

type State string

const (
	StatePending   State = "pending"
	StateDelivered State = "delivered"
	StateRejected  State = "rejected"
)

type Verdict uint8

const (
	Delivered Verdict = iota + 1
	Rejected
	Retry
)

type Result struct {
	Verdict Verdict
	Reason  string
}

type Sender interface {
	Send(m *Mutation) Result
}

type Entry struct {
	Op       string
	Payload  []byte
	Coalesce string
}

type Report struct {
	Delivered int
	Rejected  int
	Blocked   bool
}

type Outbox struct {
	db         *orm.DB
	ids        model.IDGenerator
	now        func() int64
	mu         sync.Mutex
	counter    int64
	inDelivery bool
	inFlight   string
}

func New(db *orm.DB, ids model.IDGenerator, now func() int64) (*Outbox, error) {
	if db == nil || ids == nil || now == nil {
		return nil, boxError("db, ids and now are required")
	}

	m := &Mutation{}
	qb := db.Query(m).OrderBy(Mutation_.Seq).Desc().Limit(1)
	_, err := ReadOneMutation(qb, m)
	if err != nil && err != orm.ErrNotFound {
		return nil, err
	}

	var c int64
	if m != nil {
		c = m.Seq
	}

	return &Outbox{
		db:      db,
		ids:     ids,
		now:     now,
		counter: c,
	}, nil
}

func (o *Outbox) Enqueue(e Entry) (id string, err error) {
	if e.Op == "" {
		return "", boxError("entry op is required")
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	var existing *Mutation
	if e.Coalesce != "" {
		existing = &Mutation{}
		qb := o.db.Query(existing).
			Where(Mutation_.Op).Eq(e.Op).
			Where(Mutation_.Coalesce).Eq(e.Coalesce).
			Where(Mutation_.State).Eq(string(StatePending)).
			Limit(1)

		_, err = ReadOneMutation(qb, existing)
		if err != nil {
			if err == orm.ErrNotFound {
				existing = nil
			} else {
				return "", err
			}
		}
	}

	if existing != nil && existing.Id != o.inFlight {
		existing.Payload = e.Payload
		existing.Reason = ""
		err = o.db.UpdateFields(existing, []string{Mutation_.Payload, Mutation_.Reason}, orm.Eq(Mutation_.Id, existing.Id))
		if err != nil {
			return "", err
		}
		return existing.Id, nil
	}

	o.counter++
	newMut := &Mutation{
		Id:            o.ids.NewID(),
		Seq:           o.counter,
		Op:            e.Op,
		Payload:       e.Payload,
		Coalesce:      e.Coalesce,
		State:         string(StatePending),
		Attempts:      0,
		NextAttemptAt: 0,
		DeliveredAt:   0,
		CreatedAt:     o.now(),
	}

	err = o.db.Create(newMut)
	if err != nil {
		return "", err
	}

	return newMut.Id, nil
}

func (o *Outbox) Deliver(s Sender) (Report, error) {
	o.mu.Lock()
	if o.inDelivery {
		o.mu.Unlock()
		return Report{}, boxError("delivery already in progress")
	}
	o.inDelivery = true
	o.mu.Unlock()

	defer func() {
		o.mu.Lock()
		o.inDelivery = false
		o.mu.Unlock()
	}()

	var rep Report
	var handled []string

	for {
		o.mu.Lock()
		m := &Mutation{}
		qb := o.db.Query(m).
			Where(Mutation_.State).Eq(string(StatePending)).
			OrderBy(Mutation_.Seq).Asc().
			Limit(1)

		_, err := ReadOneMutation(qb, m)
		if err != nil {
			o.mu.Unlock()
			if err == orm.ErrNotFound {
				return rep, nil
			}
			return rep, err
		}

		if m.NextAttemptAt > o.now() {
			rep.Blocked = true
			o.mu.Unlock()
			return rep, nil
		}

		for _, h := range handled {
			if h == m.Id {
				o.mu.Unlock()
				return rep, boxError("state update did not persist for mutation " + m.Id)
			}
		}
		handled = append(handled, m.Id)

		o.inFlight = m.Id
		o.mu.Unlock()

		res := s.Send(m)

		o.mu.Lock()
		o.inFlight = ""
		switch res.Verdict {
		case Delivered:
			m.State = string(StateDelivered)
			m.DeliveredAt = o.now()
			m.Reason = ""
			rep.Delivered++
			err = o.db.UpdateFields(m, []string{Mutation_.State, Mutation_.DeliveredAt, Mutation_.Reason}, orm.Eq(Mutation_.Id, m.Id))
		case Rejected:
			m.State = string(StateRejected)
			m.Reason = res.Reason
			rep.Rejected++
			err = o.db.UpdateFields(m, []string{Mutation_.State, Mutation_.Reason}, orm.Eq(Mutation_.Id, m.Id))
		case Retry:
			fallthrough
		default:
			m.Attempts++
			m.NextAttemptAt = o.now() + backoff(m.Attempts)

			if res.Verdict != Retry {
				m.Reason = "outbox: sender returned an unknown verdict"
			} else {
				m.Reason = res.Reason
			}

			err = o.db.UpdateFields(m, []string{Mutation_.State, Mutation_.Attempts, Mutation_.NextAttemptAt, Mutation_.Reason}, orm.Eq(Mutation_.Id, m.Id))
			o.mu.Unlock()

			rep.Blocked = true
			if err != nil {
				return rep, err
			}
			return rep, nil
		}
		o.mu.Unlock()

		if err != nil {
			return rep, err
		}
	}
}

const (
	minBackoff int64 = 1000000000 // 1s
	maxBackoff int64 = 60000000000 // 60s
)

func backoff(attempts int64) int64 {
	if attempts <= 0 {
		return minBackoff
	}
	shift := attempts - 1
	if shift >= 62 {
		return maxBackoff
	}

	val := minBackoff * (1 << shift)
	if val > maxBackoff || val <= 0 {
		return maxBackoff
	}
	return val
}

func (o *Outbox) RetryNow() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	qb := o.db.Query(&Mutation{}).Where(Mutation_.State).Eq(string(StatePending))
	list, err := ReadAllMutation(qb)
	if err != nil {
		return err
	}

	for _, m := range list {
		if m.NextAttemptAt != 0 {
			m.NextAttemptAt = 0
			err = o.db.UpdateFields(m, []string{Mutation_.NextAttemptAt}, orm.Eq(Mutation_.Id, m.Id))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *Outbox) Pending() (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	qb := o.db.Query(&Mutation{}).Where(Mutation_.State).Eq(string(StatePending))
	list, err := ReadAllMutation(qb)
	if err != nil {
		return 0, err
	}
	return len(list), nil
}

func (o *Outbox) Rejected() ([]Mutation, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	qb := o.db.Query(&Mutation{}).
		Where(Mutation_.State).Eq(string(StateRejected)).
		OrderBy(Mutation_.Seq).Asc()

	list, err := ReadAllMutation(qb)
	if err != nil {
		return nil, err
	}

	res := make([]Mutation, len(list))
	for i, m := range list {
		res[i] = *m
	}
	return res, nil
}

func (o *Outbox) Dismiss(id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	m := &Mutation{}
	qb := o.db.Query(m).Where(Mutation_.Id).Eq(id).Limit(1)
	_, err := ReadOneMutation(qb, m)
	if err != nil {
		if err == orm.ErrNotFound {
			return boxError("only a rejected mutation can be dismissed")
		}
		return err
	}

	if m.State != string(StateRejected) {
		return boxError("only a rejected mutation can be dismissed")
	}

	return o.db.Delete(m, orm.Eq(Mutation_.Id, id))
}

func (o *Outbox) Redeliver(since int64) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	qb := o.db.Query(&Mutation{}).
		Where(Mutation_.State).Eq(string(StateDelivered)).
		Where(Mutation_.DeliveredAt).Gte(since)

	list, err := ReadAllMutation(qb)
	if err != nil {
		return 0, err
	}

	for _, m := range list {
		m.State = string(StatePending)
		m.DeliveredAt = 0
		m.Attempts = 0
		m.NextAttemptAt = 0

		err = o.db.UpdateFields(m, []string{
			Mutation_.State,
			Mutation_.DeliveredAt,
			Mutation_.Attempts,
			Mutation_.NextAttemptAt,
		}, orm.Eq(Mutation_.Id, m.Id))

		if err != nil {
			return 0, err
		}
	}

	return len(list), nil
}

func (o *Outbox) Prune(before int64) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	qb := o.db.Query(&Mutation{}).
		Where(Mutation_.State).Eq(string(StateDelivered)).
		Where(Mutation_.DeliveredAt).Lt(before)

	list, err := ReadAllMutation(qb)
	if err != nil {
		return 0, err
	}

	for _, m := range list {
		err = o.db.Delete(m, orm.Eq(Mutation_.Id, m.Id))
		if err != nil {
			return 0, err
		}
	}

	return len(list), nil
}
