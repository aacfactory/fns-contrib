package specifications

import (
	"context"
	"github.com/aacfactory/fns-contrib/databases/sql"
)

func ScanRows[T any](ctx context.Context, rows sql.Rows, fields []string) (entries []T, err error) {
	spec, specErr := GetSpecification(ctx, Instance[T]())
	if specErr != nil {
		err = specErr
		return
	}
	for rows.Next() {
		generics := acquireGenerics(len(fields))
		scanErr := rows.Scan(generics...)
		if scanErr != nil {
			releaseGenerics(generics)
			err = scanErr
			return
		}
		entry := Instance[T]()
		writeErr := generics.WriteTo(spec, fields, &entry)
		releaseGenerics(generics)
		if writeErr != nil {
			err = scanErr
			return
		}
		entries = append(entries, entry)
	}
	return
}
