package generators

import "github.com/aacfactory/fns/cmd/generates/modules"

func Generators() []modules.FnAnnotationCodeWriter {
	return []modules.FnAnnotationCodeWriter{
		&UseWriter{},
		&TransactionWriter{},
	}
}
