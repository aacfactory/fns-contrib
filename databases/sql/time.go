package sql

import (
	"fmt"
	"reflect"
	"time"
)

func TimeNow() Date {
	return NewTimeFromTime(time.Now())
}

func NewTime(hour int, min int, sec int) Time {
	return Time{
		Hour:    hour,
		Minutes: min,
		Second:  sec,
	}
}

func NewTimeFromTime(t time.Time) Date {
	return NewDate(t.Year(), t.Month(), t.Day())
}

type Time struct {
	Hour    int
	Minutes int
	Second  int
}

func (t *Time) UnmarshalJSON(p []byte) error {
	if p == nil || len(p) < 3 {
		return nil
	}
	p = p[1 : len(p)-1]
	v, parseErr := time.Parse("15:04:05", string(p))
	if parseErr != nil {
		return fmt.Errorf("nnmarshal %s failed, layout of date must be 15:04:05, %v", string(p), parseErr)
	}
	t.Hour = v.Hour()
	t.Minutes = v.Minute()
	t.Second = v.Second()
	return nil
}

func (t Time) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", t.ToTime().Format("15:04:05"))), nil
}

func (t Time) ToTime() time.Time {
	if t.Hour < 0 || t.Hour > 23 {
		t.Hour = 0
	}
	if t.Minutes < 0 || t.Minutes > 59 {
		t.Minutes = 0
	}
	if t.Second < 0 || t.Second > 59 {
		t.Second = 0
	}
	return time.Date(1, 1, 1, t.Hour, t.Minutes, t.Second, 0, time.Local)
}

func (t Time) ToSQL() string {
	return fmt.Sprintf("ttt:%s", t.ToTime().Format(time.RFC3339))
}

func (t Time) IsZero() (ok bool) {
	ok = t.Hour == 0 && t.Minutes == 0 && t.Second == 0
	return
}

func (t Time) String() string {
	return t.ToTime().Format("15:04:05")
}

func (t *Time) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	x := ""
	switch src.(type) {
	case string:
		x = src.(string)
	case []byte:
		x = string(src.([]byte))
	case time.Time:
		v := src.(time.Time)
		t.Hour = v.Hour()
		t.Minutes = v.Minute()
		t.Second = v.Second()
		return nil
	default:
		return fmt.Errorf("scan sql raw value failed for %v is not supported", reflect.TypeOf(src).String())
	}
	if x == "" {
		return nil
	}
	v, parseErr := time.Parse("15:04:05", x)
	if parseErr != nil {
		return fmt.Errorf("scan sql date value failed, parse %s failed, %v", x, parseErr)
	}
	t.Hour = v.Hour()
	t.Minutes = v.Minute()
	t.Second = v.Second()
	return nil
}
