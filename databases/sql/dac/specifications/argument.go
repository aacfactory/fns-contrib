package specifications

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/json"
	"reflect"
)

func (spec *Specification) Arguments(instance any, fieldNames []string) (arguments []any, err error) {
	rv := reflect.Indirect(reflect.ValueOf(instance))
	for _, fieldName := range fieldNames {
		var target *Column
		for _, column := range spec.Columns {
			if column.Field == fieldName {
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
			fv := rv.FieldByName(target.Field)
			arguments = append(arguments, fv.Interface())
			break
		case Reference:
			fv := rv.FieldByName(target.Field)
			if fv.Type().Kind() == reflect.Ptr {
				if fv.IsNil() {
					arguments = append(arguments, nil)
					break
				}
				fv = fv.Elem()
			}
			refField, mapping, ok := target.Reference()
			if !ok {
				err = errors.Warning("sql: field is not reference").WithMeta("table", rv.Type().String()).WithMeta("field", target.Field)
				return
			}
			ref := fv.Interface()
			argument, argumentErr := mapping.ArgumentByField(ref, refField)
			if argumentErr != nil {
				err = errors.Warning("sql: get field value failed").WithCause(argumentErr).WithMeta("table", rv.Type().String()).WithMeta("field", target.Field)
				return
			}
			arguments = append(arguments, argument)
			break
		case Json:
			fv := rv.FieldByName(target.Field)
			if fv.Type().Kind() == reflect.Ptr {
				if fv.IsNil() {
					arguments = append(arguments, json.NullBytes)
					break
				}
				fv = fv.Elem()
			}
			argument, argumentErr := json.Marshal(fv.Interface())
			if argumentErr != nil {
				err = errors.Warning("sql: encode field value failed").WithCause(argumentErr).WithMeta("table", rv.Type().String()).WithMeta("field", target.Field)
				return
			}
			arguments = append(arguments, json.RawMessage(argument))
			break
		default:
			err = errors.Warning("sql: field can not as argument").WithMeta("table", rv.Type().String()).WithMeta("field", target.Field)
			return
		}
	}
	return
}

func (spec *Specification) ArgumentByField(instance any, field string) (argument any, err error) {
	rv := reflect.Indirect(reflect.ValueOf(instance))
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
		fv := rv.FieldByName(target.Field)
		argument = fv.Interface()
		break
	case Reference:
		refField, mapping, ok := target.Reference()
		if !ok {
			err = errors.Warning("sql: field is not reference").WithMeta("table", rv.Type().String()).WithMeta("field", target.Field)
			return
		}
		fv := rv.FieldByName(target.Field)
		if fv.Type().Kind() == reflect.Ptr {
			if fv.IsNil() {
				return
			}
			fv = fv.Elem()
		}
		ref := fv.Interface()
		argument, err = mapping.ArgumentByField(ref, refField)
		if err != nil {
			err = errors.Warning("sql: get field value failed").WithCause(err).WithMeta("table", rv.Type().String()).WithMeta("field", target.Field)
			return
		}
		break
	case Json:
		fv := rv.FieldByName(target.Field)
		encode, encodeErr := json.Marshal(fv.Interface())
		if encodeErr != nil {
			err = errors.Warning("sql: encode field value failed").WithCause(encodeErr).WithMeta("table", rv.Type().String()).WithMeta("field", target.Field)
			return
		}
		argument = json.RawMessage(encode)
		break
	default:
		err = errors.Warning("sql: field can not as argument").WithMeta("table", rv.Type().String()).WithMeta("field", target.Field)
		return
	}
	return
}
