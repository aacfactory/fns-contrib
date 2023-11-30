package specifications

import (
	stdsql "database/sql"
	"github.com/aacfactory/fns/commons/times"
	"github.com/aacfactory/json"
	"time"
	"unsafe"
)

type ScanValue interface {
	Scan(src any) (err error)
	Value() (v any, valid bool)
}

type DateValue struct {
	valid bool
	value times.Date
}

func (v *DateValue) Scan(src any) (err error) {
	switch ss := src.(type) {
	case string:
		t, parseErr := time.Parse("2006-01-02", ss)
		if parseErr != nil {
			err = parseErr
			return
		}
		v.value = times.DataOf(t)
		v.valid = true
		break
	case []byte:
		t, parseErr := time.Parse("2006-01-02", unsafe.String(unsafe.SliceData(ss), len(ss)))
		if parseErr != nil {
			err = parseErr
			return
		}
		v.value = times.DataOf(t)
		v.valid = true
		break
	case time.Time:
		v.value = times.DataOf(ss)
		v.valid = true
		break
	case nil:
		break
	default:
		sv := stdsql.NullTime{}
		err = sv.Scan(src)
		if err != nil {
			return
		}
		if sv.Valid {
			v.value = times.DataOf(sv.Time)
			v.valid = true
		}
		break
	}
	return
}

func (v *DateValue) Value() (value any, valid bool) {
	valid = v.valid
	if valid {
		value = v.value
	}
	return
}

type TimeValue struct {
	valid bool
	value times.Time
}

func (v *TimeValue) Scan(src any) (err error) {
	switch ss := src.(type) {
	case string:
		t, parseErr := time.Parse("15:04:05", ss)
		if parseErr != nil {
			err = parseErr
			return
		}
		v.value = times.TimeOf(t)
		v.valid = true
		break
	case []byte:
		t, parseErr := time.Parse("15:04:05", unsafe.String(unsafe.SliceData(ss), len(ss)))
		if parseErr != nil {
			err = parseErr
			return
		}
		v.value = times.TimeOf(t)
		v.valid = true
		break
	case time.Time:
		v.value = times.TimeOf(ss)
		v.valid = true
		break
	case nil:
		break
	default:
		sv := stdsql.NullTime{}
		err = sv.Scan(src)
		if err != nil {
			return
		}
		if sv.Valid {
			v.value = times.TimeOf(sv.Time)
			v.valid = true
		}
		break
	}
	return
}

func (v *TimeValue) Value() (value any, valid bool) {
	valid = v.valid
	if valid {
		value = v.value
	}
	return
}

type JsonValue struct {
	valid bool
	value json.RawMessage
}

func (v *JsonValue) Scan(src any) (err error) {
	if v.value == nil {
		v.value = json.RawMessage{}
	}
	err = v.value.Scan(src)
	if err != nil {
		return
	}
	v.valid = v.value.Valid()
	return
}

func (v *JsonValue) Value() (value any, valid bool) {
	valid = v.valid
	if valid {
		value = v.value
	}
	return
}
