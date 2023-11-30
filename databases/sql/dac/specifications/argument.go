package specifications

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/json"
	"reflect"
)

func (spec *Specification) Arguments(instance Table, fieldIndexes []int) (arguments []any, err error) {
	rv := reflect.ValueOf(instance)
	for _, index := range fieldIndexes {
		var target *Column
		for _, column := range spec.Columns {
			if column.FieldIdx == index {
				target = column
				break
			}
		}
		if target == nil {
			err = errors.Warning("sql: field was not found").WithMeta("table", rv.Type().String())
			return
		}
		switch target.Kind {
		case Normal, Pk, Acb, Act, Amb, Amt, Adb, Adt, Aol:
			fv := rv.Field(target.FieldIdx)
			arguments = append(arguments, fv.Interface())
			break
		case Reference:
			fv := rv.Field(target.FieldIdx)
			if fv.Type().Kind() == reflect.Ptr {
				if fv.IsNil() {
					fv = reflect.New(reflect.TypeOf(target.ZeroValue()))
				}
				fv = fv.Elem()
			}
			_, refField, mapping, ok := target.Reference()
			if !ok {
				err = errors.Warning("sql: field is not reference").WithMeta("table", rv.Type().String()).WithMeta("field", target.Field)
				return
			}
			ref, isTable := fv.Interface().(Table)
			if !isTable {
				err = errors.Warning("sql: type of reference field is not Table").WithMeta("table", rv.Type().String()).WithMeta("field", target.Field)
				return
			}
			argument, argumentErr := mapping.ArgumentByField(ref, refField)
			if argumentErr != nil {
				err = errors.Warning("sql: get field value failed").WithCause(argumentErr).WithMeta("table", rv.Type().String()).WithMeta("field", target.Field)
				return
			}
			arguments = append(arguments, argument)
		case Json:
			fv := rv.Field(target.FieldIdx)
			argument, argumentErr := json.Marshal(fv.Interface())
			if argumentErr != nil {
				err = errors.Warning("sql: encode field value failed").WithCause(argumentErr).WithMeta("table", rv.Type().String()).WithMeta("field", target.Field)
				return
			}
			arguments = append(arguments, json.RawMessage(argument))
		default:
			err = errors.Warning("sql: field can not as argument").WithMeta("table", rv.Type().String()).WithMeta("field", target.Field)
			return
		}
	}
	return
}

func (spec *Specification) ArgumentByField(instance Table, field string) (argument any, err error) {
	rv := reflect.ValueOf(instance)
	var target *Column
	for _, column := range spec.Columns {
		if column.Field == field {
			target = column
			break
		}
	}
	if target == nil {
		err = errors.Warning("sql: field was not found").WithMeta("table", rv.Type().String()).WithMeta("field", field)
		return
	}
	switch target.Kind {
	case Normal, Pk, Acb, Act, Amb, Amt, Adb, Adt, Aol:
		fv := rv.Field(target.FieldIdx)
		argument = fv.Interface()
		break
	case Reference:
		fv := rv.Field(target.FieldIdx)
		if fv.Type().Kind() == reflect.Ptr {
			fv = fv.Elem()
		}
		_, refField, mapping, ok := target.Reference()
		if !ok {
			err = errors.Warning("sql: field is not reference").WithMeta("table", rv.Type().String()).WithMeta("field", target.Field)
			return
		}
		ref, isTable := fv.Interface().(Table)
		if !isTable {
			err = errors.Warning("sql: type of reference field is not Table").WithMeta("table", rv.Type().String()).WithMeta("field", target.Field)
			return
		}
		argument, err = mapping.ArgumentByField(ref, refField)
		if err != nil {
			err = errors.Warning("sql: get field value failed").WithCause(err).WithMeta("table", rv.Type().String()).WithMeta("field", target.Field)
			return
		}
	case Json:
		fv := rv.Field(target.FieldIdx)
		encode, encodeErr := json.Marshal(fv.Interface())
		if encodeErr != nil {
			err = errors.Warning("sql: encode field value failed").WithCause(encodeErr).WithMeta("table", rv.Type().String()).WithMeta("field", target.Field)
			return
		}
		argument = json.RawMessage(encode)
	default:
		err = errors.Warning("sql: field can not as argument").WithMeta("table", rv.Type().String()).WithMeta("field", target.Field)
		return
	}
	return
}
