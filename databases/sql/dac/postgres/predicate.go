package postgres

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns/commons/times"
	"github.com/aacfactory/json"
	"github.com/valyala/bytebufferpool"
	"time"
	"unsafe"
)

func ContainsJsonObject(field string, object json.RawMessage) conditions.Condition {
	return conditions.New(conditions.Predicate{
		Field:      field,
		Operator:   "@>",
		Expression: conditions.String(unsafe.String(unsafe.SliceData(object), len(object))),
	})
}

func ContainsJsonKey(field string, key string) conditions.Condition {
	return conditions.New(conditions.Predicate{
		Field:      field,
		Operator:   "?",
		Expression: conditions.String(key),
	})
}

func ContainsJsonObjectOfArray(field string, object json.RawMessage) conditions.Condition {
	return conditions.New(conditions.Predicate{
		Field:      field,
		Operator:   "?",
		Expression: conditions.String(unsafe.String(unsafe.SliceData(object), len(object))),
	})
}

func ContainsJsonObjectsOfArray(field string, all bool, elements ...any) conditions.Condition {
	values := make([]conditions.Literal, 0, 1)
	for _, element := range elements {
		if element == nil {
			values = append(values, conditions.Null())
			continue
		}
		switch ele := element.(type) {
		case string:
			values = append(values, conditions.String(ele))
			break
		case int:
			values = append(values, conditions.Int(ele))
			break
		case int16:
			values = append(values, conditions.Int64(int64(ele)))
			break
		case int32:
			values = append(values, conditions.Int64(int64(ele)))
			break
		case int64:
			values = append(values, conditions.Int64(ele))
			break
		case float32:
			values = append(values, conditions.Float(ele))
			break
		case float64:
			values = append(values, conditions.Float64(ele))
			break
		case bool:
			values = append(values, conditions.Bool(ele))
			break
		case time.Time:
			conditions.Datetime(ele)
			values = append(values, conditions.Datetime(ele))
			break
		case json.Date:
			values = append(values, conditions.String(ele.String()))
			break
		case json.Time:
			values = append(values, conditions.String(ele.String()))
			break
		case times.Date:
			values = append(values, conditions.String(ele.String()))
			break
		case times.Time:
			values = append(values, conditions.String(ele.String()))
			break
		}
	}
	op := "?|"
	if all {
		op = "?&"
	}
	buf := bytebufferpool.Get()
	_, _ = buf.Write([]byte{'A', 'R', 'R', 'A', 'Y', '['})
	for i, value := range values {
		if i > 0 {
			_, _ = buf.Write([]byte{',', ' '})
		}
		_, _ = buf.Write(value.Bytes())
	}
	_, _ = buf.Write([]byte{']'})
	p := buf.Bytes()
	bytebufferpool.Put(buf)
	return conditions.New(conditions.Predicate{
		Field:      field,
		Operator:   conditions.Operator(op),
		Expression: conditions.String(unsafe.String(unsafe.SliceData(p), len(p))),
	})
}
