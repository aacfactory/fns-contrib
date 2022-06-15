package sql

import (
	"database/sql"
	"github.com/aacfactory/json"
)

func newRows(raws *sql.Rows) (r Rows, err error) {
	colTypes, colTypesErr := raws.ColumnTypes()
	if colTypesErr != nil {
		err = colTypesErr
		return
	}
	values := make([]Row, 0, 1)
	for raws.Next() {
		columns := make([]Column, 0, 1)
		columnScanners := make([]interface{}, 0, 1)
		for _, colType := range colTypes {
			col := NewColumnScanner(colType)
			columnScanners = append(columnScanners, col)
			columns = append(columns, col.column)
		}

		scanErr := raws.Scan(columnScanners...)
		if scanErr != nil {
			err = scanErr
			return
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

func (r rows) MarshalJSON() (p []byte, err error) {
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
