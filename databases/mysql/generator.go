package mysql

import (
	"github.com/aacfactory/fns-contrib/databases/mysql/generators"
	"github.com/aacfactory/fns/cmd/generates/modules"
)

func FAG() []modules.FnAnnotationCodeWriter {
	return []modules.FnAnnotationCodeWriter{
		&generators.UseWriter{},
		&generators.TransactionWriter{},
	}
}
