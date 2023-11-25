package databases

import (
	"database/sql"
	"reflect"
)

type ColumnType struct {
	DatabaseType string
	ScanType     reflect.Type
}

type Rows interface {
	Columns() ([]string, error)
	ColumnTypes() ([]ColumnType, error)
	Next() bool
	Scan(dst ...any) error
	Close() error
}

type DefaultRows struct {
	core *sql.Rows
}

func (rows *DefaultRows) Columns() ([]string, error) {
	return rows.core.Columns()
}

func (rows *DefaultRows) ColumnTypes() ([]ColumnType, error) {
	cts, ctxErr := rows.core.ColumnTypes()
	if ctxErr != nil {
		return nil, ctxErr
	}
	types := make([]ColumnType, 0, len(cts))
	for _, ct := range cts {
		types = append(types, ColumnType{
			DatabaseType: ct.DatabaseTypeName(),
			ScanType:     ct.ScanType(),
		})
	}
	return types, nil
}

func (rows *DefaultRows) Next() bool {
	return rows.core.Next()
}

func (rows *DefaultRows) Scan(dst ...any) error {
	return rows.core.Scan(dst...)
}

func (rows *DefaultRows) Close() error {
	return rows.core.Close()
}
