package sql

import (
	db "database/sql"
	"github.com/aacfactory/json"
)

func newRows(raws *db.Rows) (r *Rows, err error) {
	colTypes, colTypesErr := raws.ColumnTypes()
	if colTypesErr != nil {
		err = colTypesErr
		return
	}
	rows := make([]*Row, 0, 1)
	for raws.Next() {
		columns := make([]*Column, 0, 1)
		columnScanners := make([]interface{}, 0, 1)
		for _, colType := range colTypes {
			column := NewColumnScanner(colType)
			columnScanners = append(columnScanners, column)
			columns = append(columns, column.Column)
		}

		scanErr := raws.Scan(columnScanners...)
		if scanErr != nil {
			err = scanErr
			return
		}

		rows = append(rows, &Row{
			columns,
		})
	}

	r = &Rows{
		values: rows,
	}
	return
}

type Rows struct {
	idx    int
	values []*Row
}

func (r Rows) MarshalJSON() (p []byte, err error) {
	if r.Empty() {
		p = []byte{'[', ']'}
		return
	}
	p, err = json.Marshal(r.values)
	return
}

func (r *Rows) UnmarshalJSON(p []byte) (err error) {
	r.values = make([]*Row, 0, 1)
	if p == nil || len(p) == 0 || len(p) == 2 {
		return
	}
	err = json.Unmarshal(p, &r.values)
	return
}

func (r *Rows) Empty() (ok bool) {
	ok = r.values == nil || len(r.values) == 0
	return
}

func (r *Rows) Size() int {
	if r.Empty() {
		return 0
	}
	return len(r.values)
}

func (r *Rows) Next() (v *Row, has bool) {
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

type Row struct {
	columns []*Column
}

func (r Row) MarshalJSON() (p []byte, err error) {
	if r.Empty() {
		p = []byte{'[', ']'}
		return
	}
	p, err = json.Marshal(r.columns)
	return
}

func (r *Row) UnmarshalJSON(p []byte) (err error) {
	r.columns = make([]*Column, 0, 1)
	if p == nil || len(p) == 0 {
		return
	}
	err = json.Unmarshal(p, &r.columns)
	return
}

func (r *Row) Empty() (ok bool) {
	ok = r.columns == nil || len(r.columns) == 0
	return
}

func (r *Row) Columns() (columns []*Column) {
	columns = r.columns
	return
}

func (r *Row) Column(name string, value interface{}) (has bool, err error) {
	if r.Empty() {
		return
	}
	for _, column := range r.columns {
		if column.Name == name {
			has = true
			err = column.Decode(value)
			return
		}
	}
	return
}
