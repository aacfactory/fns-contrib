package sql

import (
	"github.com/aacfactory/fns-contrib/databases/sql/generators"
	"github.com/aacfactory/fns/cmd/generates/modules"
)

func FAG() modules.FnAnnotationCodeWriter {
	return &generators.TransactionWriter{}
}
