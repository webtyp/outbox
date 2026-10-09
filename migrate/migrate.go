package migrate

import (
	"webtyp.com/ddl"
	"webtyp.com/outbox"
)

func Migrate(conn ddl.Execer, compiler ddl.Compiler) error {
	return ddl.New(conn, compiler).CreateTable(&outbox.Mutation{})
}
