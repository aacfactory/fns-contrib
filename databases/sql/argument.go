package sql

import (
	"bytes"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/json"
	"reflect"
)

func NewArguments() *Arguments {
	return &Arguments{
		values: make([]interface{}, 0, 1),
	}
}

type Arguments struct {
	values []interface{}
}

func (t *Arguments) Size() (n int) {
	if t.values == nil {
		return
	}
	n = len(t.values)
	return
}

func (t *Arguments) Merge(v *Arguments) *Arguments {
	if v == nil || v.values == nil || len(v.values) == 0 {
		return t
	}
	t.values = append(t.values, v.values...)
	return t
}

func (t *Arguments) Append(values ...interface{}) *Arguments {
	if t.values == nil {
		t.values = make([]interface{}, 0, 1)
	}
	if values == nil || len(values) == 0 {
		return t
	}
	for _, v := range values {
		t.values = append(t.values, v)
	}
	return t
}

func (t *Arguments) Values() (args []interface{}) {
	if t.values == nil {
		args = make([]interface{}, 0, 1)
		return
	}
	args = t.values
	return
}

func (t *Arguments) MarshalJSON() (p []byte, err error) {
	if t.values == nil || len(t.values) == 0 {
		p = []byte{'[', ']'}
	}
	vv := make([][]byte, 0, 1)
	for _, value := range t.values {
		if value == nil {
			vv = append(vv, bytex.FromString("nil:nil"))
			continue
		}
		typ := reflect.TypeOf(value)
		vt, hasVT := valueTypes[typ.String()]
		if !hasVT {
			err = errors.Warning("sql: value type was not found").WithMeta("type", typ.String())
			return
		}
		vtp, encodeErr := vt.Encode(value)
		if encodeErr != nil {
			err = errors.Warning("sql: encode value type failed").WithMeta("type", typ.String()).WithCause(encodeErr)
			return
		}
		vv = append(vv, bytes.Join([][]byte{bytex.FromString(vt.ColumnType()), vtp}, []byte{':'}))
	}
	p, err = json.Marshal(vv)
	return
}

func (t *Arguments) UnmarshalJSON(p []byte) (err error) {
	vv := make([][]byte, 0, 1)
	err = json.Unmarshal(p, &vv)
	if err != nil {
		return
	}
	if t.values == nil {
		t.values = make([]interface{}, 0, len(vv))
	}
	for _, v := range vv {
		idx := bytes.IndexByte(v, ':')
		if idx < 0 {
			err = errors.Warning("sql: invalid arguments")
			return
		}
		ct := bytex.ToString(v[0:idx])
		vt, hasVT := findValueTypeByColumnType(ct)
		if !hasVT {
			err = errors.Warning("sql: value type was not registered").WithMeta("columnType", ct)
			return
		}
		if len(v) < idx+1 {
			err = errors.Warning("sql: invalid arguments")
			return
		}
		src, decodeErr := vt.Decode(v[idx+1:])
		if decodeErr != nil {
			err = decodeErr
			return
		}
		t.values = append(t.values, src)
	}
	return
}
