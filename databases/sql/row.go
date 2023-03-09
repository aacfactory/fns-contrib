package sql

import (
	stdsql "database/sql"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/json"
)

type Row interface {
	json.Marshaler
	json.Unmarshaler
	Empty() (ok bool)
	Columns() (columns []Column)
	Column(name string, value interface{}) (has bool, err error)
}

type Rows interface {
	json.Marshaler
	json.Unmarshaler
	Empty() (ok bool)
	Size() int
	Next() (v Row, has bool)
}

func newRows(raws *stdsql.Rows) (r Rows, err error) {
	colTypes, columnTypesErr := raws.ColumnTypes()
	if columnTypesErr != nil {
		err = errors.Warning("sql: get columns types from sql rows failed").WithCause(columnTypesErr)
		return
	}
	colNames, columnNamesErr := raws.Columns()
	if columnNamesErr != nil {
		err = errors.Warning("sql: get columns names from sql rows failed").WithCause(columnNamesErr)
		return
	}
	vts := make([]ValueType, 0, 1)
	scanners := make([]any, 0, 1)
	for _, columnType := range colTypes {
		vt, hasVT := findValueTypeByDatabaseType(columnType.DatabaseTypeName())
		if !hasVT {
			err = errors.Warning("sql: value type was not registered").WithMeta("column", columnType.Name()).WithMeta("databaseType", columnType.DatabaseTypeName())
			return
		}
		scanner := vt.Scanner()
		if scanner == nil {
			err = errors.Warning("sql: scanner of value type is required").WithMeta("column", columnType.Name()).WithMeta("databaseType", columnType.DatabaseTypeName())
			return
		}
		vts = append(vts, vt)
		scanners = append(scanners, scanner)
	}
	values := make([]Row, 0, 1)
	for raws.Next() {
		scanErr := raws.Scan(scanners...)
		if scanErr != nil {
			err = errors.Warning("sql: row scan failed").WithCause(scanErr)
			return
		}

		columns := make([]Column, 0, 1)
		for i, scanner0 := range scanners {
			scanner := scanner0.(ValueScanner)
			value := scanner.Value()
			ct := ""
			var p []byte
			isNil := value == nil
			if isNil {
				ct = "nil"
				p = bytex.FromString("nil:nil")
			} else {
				vt := vts[i]
				ct = vt.ColumnType()
				p, err = vt.Encode(value)
				if err != nil {
					err = errors.Warning("sql: row scan failed").WithCause(err)
					return
				}
			}
			columns = append(columns, &column{
				Type_:         ct,
				DatabaseType_: colTypes[i].DatabaseTypeName(),
				Name_:         colNames[i],
				Value_:        p,
				Nil:           isNil,
			})
			scanner.Reset()
		}
		values = append(values, &row{
			columns,
		})
	}
	r = &rows{
		values: values,
	}
	return
}

type rows struct {
	idx    int
	values []Row
}

func (r *rows) MarshalJSON() (p []byte, err error) {
	if r.Empty() {
		p = []byte{'[', ']'}
		return
	}
	p, err = json.Marshal(r.values)
	return
}

func (r *rows) UnmarshalJSON(p []byte) (err error) {
	r.values = make([]Row, 0, 1)
	if p == nil || len(p) == 0 || len(p) == 2 {
		return
	}
	err = json.Unmarshal(p, &r.values)
	return
}

func (r *rows) Empty() (ok bool) {
	ok = r.values == nil || len(r.values) == 0
	return
}

func (r *rows) Size() int {
	if r.Empty() {
		return 0
	}
	return len(r.values)
}

func (r *rows) Next() (v Row, has bool) {
	if r.Empty() {
		return
	}
	has = r.idx < r.Size()
	if has {
		v = r.values[r.idx]
		r.idx++
	}
	return
}

type row struct {
	columns []Column
}

func (r *row) MarshalJSON() (p []byte, err error) {
	if r.Empty() {
		p = []byte{'[', ']'}
		return
	}
	p, err = json.Marshal(r.columns)
	return
}

func (r *row) UnmarshalJSON(p []byte) (err error) {
	r.columns = make([]Column, 0, 1)
	if p == nil || len(p) == 0 {
		return
	}
	err = json.Unmarshal(p, &r.columns)
	return
}

func (r *row) Empty() (ok bool) {
	ok = r.columns == nil || len(r.columns) == 0
	return
}

func (r *row) Columns() (columns []Column) {
	columns = r.columns
	return
}

func (r *row) Column(name string, value interface{}) (has bool, err error) {
	if r.Empty() {
		return
	}
	for _, col := range r.columns {
		if col.Name() == name {
			has = true
			err = col.Get(value)
			return
		}
	}
	return
}
