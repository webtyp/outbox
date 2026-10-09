package outbox

type boxError string

func (e boxError) Error() string { return "outbox: " + string(e) }
