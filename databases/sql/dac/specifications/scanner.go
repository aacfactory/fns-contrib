package specifications

import (
	"fmt"
	"github.com/aacfactory/errors"
	"reflect"
	"sync"
)

var (
	genericsPool = sync.Pool{}
)

func acquireGenerics(n int) (v Generics) {
	c := genericsPool.Get()
	if c == nil {
		v = make(Generics, n)
		for i := 0; i < n; i++ {
			v[i] = &Generic{}
		}
	} else {
		v = c.(Generics)
		vLen := len(v)
		if delta := n - vLen; delta < 0 {
			v = v[0:n]
		} else if delta > 0 {
			for i := 0; i < delta; i++ {
				v = append(v, &Generic{})
			}
		}
	}
	return
}

func releaseGenerics(vv ...Generics) {
	for _, generics := range vv {
		for _, generic := range generics {
			generic.(*Generic).Reset()
		}
		genericsPool.Put(generics)
	}
}

type Generics []any

func (generics Generics) WriteTo(spec *Specification, fieldNames []string, entryPtr any) (err error) {
	rv := reflect.Indirect(reflect.ValueOf(entryPtr))
	for i, fieldName := range fieldNames {
		column, has := spec.ColumnByField(fieldName)
		if !has {
			err = errors.Warning(fmt.Sprintf("sql: %s field was not found in %s", fieldName, spec.Key)).
				WithMeta("field", fieldName).WithMeta("table", spec.Key)
			return
		}
		fv := rv.FieldByName(fieldName)
		generic := generics[i].(*Generic)
		if generic.Valid {
			err = column.WriteValue(fv, generic.Value)
			if err != nil {
				err = errors.Warning(fmt.Sprintf("sql: write value into %s.%s field failed", spec.Key, fieldName)).WithCause(err).
					WithMeta("field", fieldName).WithMeta("table", spec.Key)
				return
			}
		}
	}
	return
}

type Generic struct {
	Valid bool
	Value any
}

func (v *Generic) Scan(src any) (err error) {
	if src == nil {
		return
	}
	v.Valid = true
	v.Value = src
	return
}

func (v *Generic) Reset() {
	v.Valid = false
	v.Value = nil
}
