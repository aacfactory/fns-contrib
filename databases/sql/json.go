package sql

import (
	stdsql "database/sql"
	"github.com/aacfactory/json"
)

type NullJson struct {
	Json  json.RawMessage
	Valid bool
}

func (v *NullJson) Scan(src interface{}) error {
	v.Json = []byte("null")
	str := &stdsql.NullString{}
	scanErr := str.Scan(src)
	if scanErr != nil {
		return scanErr
	}
	if str.String == "" {
		return nil
	}
	if json.ValidateString(str.String) {
		v.Valid = true
		v.Json = []byte(str.String)
	}
	return nil
}
