package outbox

type boxError string

func (e boxError) Error() string { return "outbox: " + string(e) }

const (
	errRequired           boxError = "db, ids and now are required"
	errOpRequired         boxError = "entry op is required"
	errDeliveryInProgress boxError = "delivery already in progress"
	errNotPersisted       boxError = "state update did not persist for mutation "
	errOnlyRejected       boxError = "only a rejected mutation can be dismissed"
	reasonUnknownVerdict           = "outbox: sender returned an unknown verdict"
)
