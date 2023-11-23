package databases

import (
	"bytes"
	"database/sql"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/json"
)

func NewColumnType(ct *sql.ColumnType) (v ColumnType) {
	name := ct.Name()
	databaseType := ct.DatabaseTypeName()
	rt := ct.ScanType()
	if rt == anyType {
		v = ColumnType{
			Name:         name,
			DatabaseType: databaseType,
			Type:         "bytes",
		}
		return
	}
	// todo
	return
}

type ColumnType struct {
	Name         string `json:"name"`
	DatabaseType string `json:"databaseType"`
	Type         string `json:"type"`
}

func NewColumn(src any) (c Column, err error) {
	if src == nil {
		return
	}
	c, err = json.Marshal(src)
	if err != nil {
		err = errors.Warning("sql: new column failed").WithCause(err)
		return
	}
	return
}

type Column []byte

func (c *Column) UnmarshalJSON(p []byte) error {
	r := json.RawMessage(*c)
	err := r.UnmarshalJSON(p)
	if err != nil {
		return err
	}
	*c = append((*c)[0:0], r...)
	return nil
}

func (c *Column) MarshalJSON() ([]byte, error) {
	return json.RawMessage(*c).MarshalJSON()
}

func (c *Column) Scan(dst any) (err error) {
	p := *c
	pLen := len(p)
	switch d := dst.(type) {
	case *sql.NullBool:
		if pLen == 0 || bytes.Equal(p, nullBytes) {
			break
		}
		d.Valid = true
		d.Bool = bytes.Equal(p, trueBytes)
		break
	case *sql.NullString:
		if pLen == 0 || bytes.Equal(p, nullBytes) {
			break
		}
		d.Valid = true
		d.String = bytex.ToString(p[1 : pLen-1])
		break
	case *sql.NullTime:
		if pLen == 0 || bytes.Equal(p, nullBytes) {
			break
		}
		d.Valid = true
		err = json.Unmarshal(p, &d.Time)
		break
	case *sql.NullInt16:
		if pLen == 0 || bytes.Equal(p, nullBytes) {
			break
		}
		d.Valid = true
		err = json.Unmarshal(p, &d.Int16)
		break
	case *sql.NullInt32:
		if pLen == 0 || bytes.Equal(p, nullBytes) {
			break
		}
		d.Valid = true
		err = json.Unmarshal(p, &d.Int32)
		break
	case *sql.NullInt64:
		if pLen == 0 || bytes.Equal(p, nullBytes) {
			break
		}
		d.Valid = true
		err = json.Unmarshal(p, &d.Int64)
		break
	case *sql.NullFloat64:
		if pLen == 0 || bytes.Equal(p, nullBytes) {
			break
		}
		d.Valid = true
		err = json.Unmarshal(p, &d.Float64)
		break
	case *sql.NullByte:
		if pLen == 0 || bytes.Equal(p, nullBytes) {
			break
		}
		d.Valid = true
		err = json.Unmarshal(p, &d.Byte)
		break
	default:
		err = json.Unmarshal(p, dst)
		break
	}
	if err != nil {
		err = errors.Warning("sql: column scan failed").WithCause(err)
		return
	}
	return
}
