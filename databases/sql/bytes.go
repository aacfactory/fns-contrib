package sql

import (
	stdsql "database/sql"
	"fmt"
	"reflect"
)

type NullRawBytes struct {
	Raw   stdsql.RawBytes
	Valid bool
}

func (v *NullRawBytes) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	switch src.(type) {
	case string:
		x := src.(string)
		if x == "" {
			return nil
		}
		v.Raw = []byte(x)
		v.Valid = true
	case []byte:
		x := src.([]byte)
		if len(x) > 0 {
			v.Raw = x
			v.Valid = true
		}
	default:
		return fmt.Errorf("scan sql raw value failed for %v is not supported", reflect.TypeOf(src).String())
	}

	return nil
}
