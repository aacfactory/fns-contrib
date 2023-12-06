package specifications

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"reflect"
)

func WriteInsertReturning[T any](ctx context.Context, rows sql.Rows, returning []string, entries []T) (affected int64, err error) {
	spec, specErr := GetSpecification(ctx, Instance[T]())
	if specErr != nil {
		err = errors.Warning("sql: write returning value into entries failed").WithCause(specErr)
		return
	}
	multiGenerics := make([]Generics, 0, len(entries))
	for rows.Next() {
		generics := acquireGenerics(len(returning))
		scanErr := rows.Scan(generics...)
		if scanErr != nil {
			releaseGenerics(generics)
			err = errors.Warning("sql: write returning value into entries failed").WithCause(scanErr)
			return
		}
		multiGenerics = append(multiGenerics, generics)
		affected++
	}

	if affected == int64(len(entries)) {
		for i, entry := range entries {
			row := multiGenerics[i]
			wErr := row.WriteTo(spec, returning, &entry)
			if wErr != nil {
				releaseGenerics(multiGenerics...)
				err = errors.Warning("sql: write returning value into entries failed").WithCause(wErr)
				return
			}
			entries[i] = entry
		}
		releaseGenerics(multiGenerics...)
		return
	}

	// todo 从上一个找到的位置开始，因为是顺序的
	if len(spec.Conflicts) > 0 {
		conflicts, conflictsErr := spec.ConflictColumns()
		if conflictsErr != nil {
			releaseGenerics(multiGenerics...)
			err = errors.Warning("sql: write returning value into entries failed").WithCause(conflictsErr)
			return
		}
		pos := len(returning)
		tmpConflicts := make([]*Column, 0, len(conflicts))
		for i, fieldName := range returning {
			matched := -1
			for j, conflict := range conflicts {
				if conflict.Field == fieldName {
					matched = j
					if pos > i {
						pos = i
					}
					break
				}
			}
			if matched > 0 {
				tmpConflicts = append(tmpConflicts, conflicts[matched])
			}
		}
		conflicts = tmpConflicts
		if pos == len(returning) {
			releaseGenerics(multiGenerics...)
			err = errors.Warning("sql: write returning value into entries failed").WithCause(fmt.Errorf("there is no conflict column in returning"))
			return
		}
		if pos == 0 {
			releaseGenerics(multiGenerics...)
			err = errors.Warning("sql: write returning value into entries failed").WithCause(fmt.Errorf("there is no valid column in returning"))
			return
		}

		for _, row := range multiGenerics {
			items := row[0:pos]
			conflictValues := row[pos:]
			for i, entry := range entries {
				rv := reflect.Indirect(reflect.ValueOf(&entry))
				matched := 0
				for j, value := range conflictValues {
					if reflect.Indirect(rv.FieldByName(conflicts[j].Field)).Equal(reflect.Indirect(reflect.ValueOf(value.(*Generic).Value))) {
						matched++
					}
				}
				if matched == len(conflictValues) {
					wErr := items.WriteTo(spec, returning, &entry)
					if wErr != nil {
						releaseGenerics(multiGenerics...)
						err = errors.Warning("sql: write returning value into entries failed").WithCause(wErr)
						return
					}
					entries[i] = entry
				}
			}
		}
		releaseGenerics(multiGenerics...)
	}
	return
}
