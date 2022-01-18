package sql

import (
	"database/sql"
	stdJson "encoding/json"
	"fmt"
	"github.com/aacfactory/json"
	"reflect"
	"strconv"
	"time"
)

func NewTuple() *Tuple {
	return &Tuple{
		values: make([]string, 0, 1),
	}
}

type Tuple struct {
	values []string
}

func (t *Tuple) Merge(v *Tuple) *Tuple {
	if v == nil || v.values == nil || len(v.values) == 0 {
		return t
	}
	t.values = append(t.values, v.values...)
	return t
}

func (t *Tuple) Append(values ...interface{}) *Tuple {
	if t.values == nil {
		t.values = make([]string, 0, 1)
	}
	if values == nil {
		return t
	}
	for i, v := range values {
		if v == nil {
			t.values = append(t.values, "nil:<nil>")
			continue
		}
		switch v.(type) {
		case string:
			t.values = append(t.values, fmt.Sprintf("sss:%s", v.(string)))
		case sql.NullString:
			x := v.(sql.NullString)
			if x.Valid {
				t.values = append(t.values, fmt.Sprintf("sss:%v", x.String))
			} else {
				t.values = append(t.values, "nil:<nil>")
			}
		case []byte:
			t.values = append(t.values, fmt.Sprintf("bbb:%s", string(v.([]byte))))
		case json.RawMessage:
			t.values = append(t.values, fmt.Sprintf("bbb:%s", string(v.(json.RawMessage))))
		case stdJson.RawMessage:
			t.values = append(t.values, fmt.Sprintf("bbb:%s", string(v.(stdJson.RawMessage))))
		case int, int8, int16, int32, int64, uint, uint16, uint32, uint64:
			t.values = append(t.values, fmt.Sprintf("int:%v", v))
		case sql.NullInt16:
			x := v.(sql.NullInt16)
			if x.Valid {
				t.values = append(t.values, fmt.Sprintf("int:%v", x.Int16))
			} else {
				t.values = append(t.values, "nil:<nil>")
			}
		case sql.NullInt32:
			x := v.(sql.NullInt32)
			if x.Valid {
				t.values = append(t.values, fmt.Sprintf("int:%v", x.Int32))
			} else {
				t.values = append(t.values, "nil:<nil>")
			}
		case sql.NullInt64:
			x := v.(sql.NullInt64)
			if x.Valid {
				t.values = append(t.values, fmt.Sprintf("int:%v", x.Int64))
			} else {
				t.values = append(t.values, "nil:<nil>")
			}
		case byte:
			t.values = append(t.values, fmt.Sprintf("byt:%v", v.(uint8)))
		case sql.NullByte:
			x := v.(sql.NullByte)
			if x.Valid {
				t.values = append(t.values, fmt.Sprintf("byt:%v", x.Byte))
			} else {
				t.values = append(t.values, "nil:<nil>")
			}
		case float32:
			t.values = append(t.values, fmt.Sprintf("f64:%f", v.(float32)))
		case float64:
			t.values = append(t.values, fmt.Sprintf("f64:%f", v.(float64)))
		case sql.NullFloat64:
			x := v.(sql.NullFloat64)
			if x.Valid {
				t.values = append(t.values, fmt.Sprintf("f64:%v", x.Float64))
			} else {
				t.values = append(t.values, "nil:<nil>")
			}
		case bool:
			b := v.(bool)
			if b {
				t.values = append(t.values, "boo:true")
			} else {
				t.values = append(t.values, "boo:false")
			}
		case sql.NullBool:
			x := v.(sql.NullBool)
			if x.Valid {
				t.values = append(t.values, fmt.Sprintf("boo:%v", x.Bool))
			} else {
				t.values = append(t.values, "nil:<nil>")
			}
		case time.Time:
			x := v.(time.Time)
			t.values = append(t.values, fmt.Sprintf("ttt:%s", x.Format(time.RFC3339)))
		case json.Date:
			x := v.(json.Date)
			t.values = append(t.values, fmt.Sprintf("ttt:%s", x.ToTime().Format(time.RFC3339)))
		case json.Time:
			x := v.(json.Time)
			t.values = append(t.values, fmt.Sprintf("ttt:%s", time.Time(x).Format(time.RFC3339)))
		case sql.NullTime:
			x := v.(sql.NullTime)
			if x.Valid {
				t.values = append(t.values, fmt.Sprintf("ttt:%v", x.Time.Format(time.RFC3339)))
			} else {
				t.values = append(t.values, "nil:<nil>")
			}
		case time.Duration:
			x := v.(time.Duration)
			t.values = append(t.values, fmt.Sprintf("int:%d", x.Milliseconds()))
		default:
			panic(fmt.Errorf("fns SQL Tuple: appended %d of values type(%s) is not supported", i, reflect.TypeOf(v).String()))
			return t
		}
	}
	return t
}

func (t *Tuple) mapToSQLArgs() (args []interface{}) {
	args = make([]interface{}, 0, 1)
	if t.values == nil || len(t.values) == 0 {
		return
	}
	for _, v := range t.values {
		kind := v[0:3]
		value := v[4:]
		switch kind {
		case "nil":
			args = append(args, nil)
		case "sss":
			args = append(args, value)
		case "bbb":
			args = append(args, []byte(value))
		case "int":
			x, _ := strconv.Atoi(value)
			args = append(args, x)
		case "byt":
			args = append(args, value[0])
		case "f64":
			x, _ := strconv.ParseFloat(value, 10)
			args = append(args, x)
		case "boo":
			if value == "true" {
				args = append(args, true)
			} else {
				args = append(args, false)
			}
		case "ttt":
			x, _ := time.Parse(time.RFC3339, value)
			args = append(args, x)
		default:
			args = append(args, []byte(value))
		}
	}
	return
}

func (t Tuple) MarshalJSON() (p []byte, err error) {
	p, err = json.Marshal(t.values)
	return
}

func (t *Tuple) UnmarshalJSON(p []byte) (err error) {
	values := make([]string, 0, 1)
	err = json.Unmarshal(p, &values)
	if err != nil {
		return
	}
	t.values = values
	return
}
