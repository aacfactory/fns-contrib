package specifications

import (
	"github.com/aacfactory/fns/context"
)

func BuildInsert[T Table](ctx context.Context, entries ...T) (method Method, query []byte, arguments []any, returning []int, err error) {
	dialect, dialectErr := LoadDialect(ctx)
	if dialectErr != nil {
		err = dialectErr
		return
	}
	t := TableInstance[T]()
	spec, specErr := GetSpecification(ctx, t)
	if specErr != nil {
		err = specErr
		return
	}
	var fields []int
	method, query, fields, returning, err = dialect.Insert(Todo(ctx, t, dialect), spec, len(entries))
	if err != nil {
		return
	}
	for _, entry := range entries {
		args, argsErr := spec.Arguments(entry, fields)
		if argsErr != nil {
			err = argsErr
			return
		}
		arguments = append(arguments, args...)
	}
	return
}
