package specifications

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
)

func WriteInsertReturning[T any](ctx context.Context, rows sql.Rows, returning []string, entries []T) (affected int64, err error) {
	spec, specErr := GetSpecification(ctx, Instance[T]())
	if specErr != nil {
		err = errors.Warning("sql: write returning value into entries failed").WithCause(specErr)
		return
	}
	i := 0
	for rows.Next() {
		generics := acquireGenerics(len(returning))
		scanErr := rows.Scan(generics...)
		if scanErr != nil {
			releaseGenerics(generics)
			err = errors.Warning("sql: write returning value into entries failed").WithCause(scanErr)
			return
		}
		entry := entries[i]
		wErr := generics.WriteTo(spec, returning, &entry)
		if wErr != nil {
			releaseGenerics(generics)
			err = errors.Warning("sql: write returning value into entries failed").WithCause(wErr)
			return
		}
		entries[i] = entry
		affected++
		i++
	}
	return
}
