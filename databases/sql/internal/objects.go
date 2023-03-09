package internal

import (
	"fmt"
	"reflect"
)

func CopyInterface(dst interface{}, src interface{}) (err error) {
	if dst == nil {
		err = fmt.Errorf("copy failed for dst is nil")
		return
	}
	dpv := reflect.ValueOf(dst)
	if dpv.Kind() != reflect.Ptr {
		err = fmt.Errorf("copy failed for type of dst is not ptr")
		return
	}
	sv := reflect.ValueOf(src)
	dv := reflect.Indirect(dpv)
	if sv.Kind() == reflect.Ptr {
		if sv.IsNil() {
			return
		}
		sv = sv.Elem()
	}
	if sv.IsValid() && sv.Type().AssignableTo(dv.Type()) {
		dv.Set(sv)
		return
	}
	if dv.Kind() == sv.Kind() && sv.Type().ConvertibleTo(dv.Type()) {
		dv.Set(sv.Convert(dv.Type()))
		return
	}
	err = fmt.Errorf("copy failed for type is not matched")
	return
}
