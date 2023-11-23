package databases

import (
	"database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/json"
)

type Row []Column

func NewRows(raw *sql.Rows) (v Rows, err error) {
	cts, ctsErr := raw.ColumnTypes()
	if ctsErr != nil {
		err = errors.Warning("sql: new rows failed").WithCause(ctsErr)
		return
	}
	columnLen := len(cts)
	columnTypes := make([]ColumnType, 0, columnLen)
	for _, ct := range cts {
		columnTypes = append(columnTypes, NewColumnType(ct))
	}
	v = Rows{
		transferred: false,
		idx:         0,
		raw:         raw,
		columnTypes: columnTypes,
		columnLen:   columnLen,
		values:      nil,
		size:        0,
	}
	return
}

type Rows struct {
	transferred bool
	idx         int
	raw         *sql.Rows
	columnTypes []ColumnType
	columnLen   int
	values      []Row
	size        int
}

func (rows *Rows) MarshalJSON() (p []byte, err error) {
	if rows.transferred {
		tr := transferRows{
			ColumnTypes: rows.columnTypes,
			Values:      rows.values,
		}
		p, err = json.Marshal(tr)
		return
	}
	if rows.idx != 0 {
		err = errors.Warning("sql: encode rows failed").WithCause(fmt.Errorf("rows has been used"))
		return
	}
	rows.values = make([]Row, 0, 1)
	for rows.raw.Next() {
		dsts := make([]interface{}, 0, rows.columnLen)
		scanErr := rows.raw.Scan(dsts...)
		if scanErr != nil {
			err = errors.Warning("sql: encode rows failed").WithCause(scanErr)
			return
		}
		row := make(Row, 0, rows.columnLen)
		for _, dst := range dsts {
			column, columnErr := NewColumn(dst)
			if columnErr != nil {
				err = errors.Warning("sql: encode rows failed").WithCause(columnErr)
				return
			}
			row = append(row, column)
		}
		rows.values = append(rows.values, row)
	}
	rows.transferred = true
	rows.size = len(rows.values)
	tr := transferRows{
		ColumnTypes: rows.columnTypes,
		Values:      rows.values,
	}
	p, err = json.Marshal(tr)
	return
}

func (rows *Rows) UnmarshalJSON(p []byte) (err error) {
	tr := transferRows{}
	err = json.Unmarshal(p, &tr)
	if err != nil {
		return
	}
	rows.idx = 0
	rows.columnTypes = tr.ColumnTypes
	rows.columnLen = len(rows.columnTypes)
	rows.values = tr.Values
	rows.size = len(rows.values)
	rows.transferred = true
	return
}

func (rows *Rows) Next() (ok bool) {
	if rows.raw != nil {
		ok = rows.raw.Next()
		return
	}
	ok = rows.idx < rows.size
	if ok {
		rows.idx++
	}
	return
}

func (rows *Rows) Scan(dst ...any) (err error) {
	if rows.raw != nil {
		err = rows.raw.Scan(dst...)
		return
	}
	if rows.idx >= rows.size {
		return
	}
	row := rows.values[rows.idx]
	for i := 0; i < rows.columnLen; i++ {
		ct := rows.columnTypes[i]
		switch ct.Type {
		// todo
		}
	}
	return
}

type transferRows struct {
	ColumnTypes []ColumnType `json:"columnTypes"`
	Values      []Row        `json:"values"`
}
