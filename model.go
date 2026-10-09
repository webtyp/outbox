package outbox

import "webtyp.com/model"

var MutationModel = model.Definition{
	Name: "outbox_mutation",
	Fields: model.Fields{
		{Name: "id", Type: model.Text(), DB: &model.FieldDB{PK: true}},
		{Name: "seq", Type: model.Int(), NotNull: true},
		{Name: "op", Type: model.Text(), NotNull: true},
		{Name: "payload", Type: model.Blob()},
		{Name: "coalesce", Type: model.Text()},
		{Name: "state", Type: model.Text(), NotNull: true},
		{Name: "attempts", Type: model.Int(), NotNull: true},
		{Name: "next_attempt_at", Type: model.Int(), NotNull: true},
		{Name: "delivered_at", Type: model.Int(), NotNull: true},
		{Name: "reason", Type: model.Text()},
		{Name: "created_at", Type: model.Int(), NotNull: true},
	},
}
