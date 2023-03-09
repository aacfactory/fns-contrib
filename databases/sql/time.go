package sql

import (
	"fmt"
	"github.com/aacfactory/json"
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

type NullTime struct {
	Valid bool
	Value Time
}

func (t *NullTime) Scan(src interface{}) error {
	v := &t.Value
	err := v.Scan(src)
	if err != nil {
		return err
	}
	t.Value = *v
	t.Valid = true
	return nil
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

func (t *Time) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", t.ToTime().Format("15:04:05"))), nil
}

func (t *Time) ToTime() time.Time {
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

func (t *Time) IsZero() (ok bool) {
	ok = t.Hour == 0 && t.Minutes == 0 && t.Second == 0
	return
}

func (t *Time) String() string {
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
		break
	case []byte:
		x = string(src.([]byte))
		break
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

func DateNow() Date {
	return NewDateFromTime(time.Now())
}

func NewDate(year int, month time.Month, day int) Date {
	return Date{
		Year:  year,
		Month: month,
		Day:   day,
	}
}

func NewDateFromTime(t time.Time) Date {
	return NewDate(t.Year(), t.Month(), t.Day())
}

func NewDateFromJsonDate(d json.Date) Date {
	return NewDate(d.Year, d.Month, d.Day)
}

type NullDate struct {
	Valid bool
	Value Date
}

func (t *NullDate) Scan(src interface{}) error {
	v := &t.Value
	err := v.Scan(src)
	if err != nil {
		return err
	}
	t.Value = *v
	t.Valid = true
	return nil
}

type Date struct {
	Year  int
	Month time.Month
	Day   int
}

func (d *Date) UnmarshalJSON(p []byte) error {
	if p == nil || len(p) < 3 {
		return nil
	}
	p = p[1 : len(p)-1]
	v, parseErr := time.Parse("2006-01-02", string(p))
	if parseErr != nil {
		return fmt.Errorf("nnmarshal %s failed, layout of date must be 2006-01-02, %v", string(p), parseErr)
	}
	d.Year = v.Year()
	d.Month = v.Month()
	d.Day = v.Day()
	return nil
}

func (d *Date) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", d.ToTime().Format("2006-01-02"))), nil
}

func (d *Date) ToTime() time.Time {
	if d.Year < 1 {
		d.Year = 1
	}
	if d.Month < 1 {
		d.Month = 1
	}
	if d.Day < 1 {
		d.Day = 1
	}
	return time.Date(d.Year, d.Month, d.Day, 0, 0, 0, 0, time.Local)
}

func (d *Date) IsZero() (ok bool) {
	ok = d.Year < 2 && d.Month < 2 && d.Day < 2
	return
}

func (d *Date) String() string {
	return d.ToTime().Format("2006-01-02")
}

func (d *Date) Scan(src interface{}) error {
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
		d.Year = v.Year()
		d.Month = v.Month()
		d.Day = v.Day()
		return nil
	default:
		return fmt.Errorf("scan sql raw value failed for %v is not supported", reflect.TypeOf(src).String())
	}
	if x == "" {
		return nil
	}
	v, parseErr := time.Parse("2006-01-02", x)
	if parseErr != nil {
		return fmt.Errorf("scan sql date value failed, parse %s failed, %v", x, parseErr)
	}
	d.Year = v.Year()
	d.Month = v.Month()
	d.Day = v.Day()
	return nil
}
