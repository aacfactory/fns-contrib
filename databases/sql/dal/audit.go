package dal

import (
	"context"
	"fmt"
	"github.com/aacfactory/fns/service"
	"reflect"
	"time"
)

func tryFillByField(ctx context.Context, rv reflect.Value, field *Field) (err error) {
	var by interface{}
	byStringTypeKind := false
	byField := rv.Elem().FieldByName(field.Name())
	if byField.Type().Kind() == reflect.String {
		byStringTypeKind = true
	}
	by = byField.Interface()
	hasByValue := false
	if byStringTypeKind {
		byString := by.(string)
		if byString == "" {
			request, hasRequest := service.GetRequest(ctx)
			if hasRequest {
				userId := request.User().Id()
				if userId != "" {
					byString = userId
					rv.Elem().FieldByName(field.Name()).SetString(byString)
					hasByValue = true
				}
			}
		} else {
			hasByValue = true
		}
	} else {
		byInt := reflect.ValueOf(by).Int()
		if byInt <= 0 {
			request, hasRequest := service.GetRequest(ctx)
			if hasRequest {
				userId := request.User().Id()
				if userId != "" {
					byInt = request.User().IntId()
					rv.Elem().FieldByName(field.Name()).SetInt(byInt)
					hasByValue = true
				}
			}
		} else {
			hasByValue = true
		}
	}
	if !hasByValue {
		err = fmt.Errorf("value of audit user column value is needed")
		return
	}
	return
}

func tryFillATField(rv reflect.Value, field *Field) (err error) {
	at := time.Time{}
	at = rv.Elem().FieldByName(field.Name()).Convert(reflect.TypeOf(at)).Interface().(time.Time)
	if at.IsZero() {
		at = time.Now()
		attField := rv.Elem().FieldByName(field.Name())
		attField.Set(reflect.ValueOf(at).Convert(attField.Type()))
	}
	return
}

func tryFillAOLField(rv reflect.Value, st *ModelStructure) (err error) {
	_, _, _, _, _, _, aol, hasAudits := st.AuditFields()
	if !hasAudits {
		return
	}
	if aol != nil {
		rf := rv.Elem().FieldByName(aol.Name())
		rf.SetInt(rf.Int() + 1)
	}
	return
}

func tryFillAOLFieldExact(rv reflect.Value, st *ModelStructure, version int64) (err error) {
	_, _, _, _, _, _, aol, hasAudits := st.AuditFields()
	if !hasAudits {
		return
	}
	if aol != nil {
		rf := rv.Elem().FieldByName(aol.Name())
		rf.SetInt(version)
	}
	return
}

func tryFillAuditCreate(ctx context.Context, rv reflect.Value, st *ModelStructure) (err error) {
	acb, act, _, _, _, _, _, hasAudits := st.AuditFields()
	if !hasAudits {
		return
	}
	if acb != nil {
		err = tryFillByField(ctx, rv, acb)
		if err != nil {
			return
		}
	}
	if act != nil {
		err = tryFillATField(rv, act)
		if err != nil {
			return
		}
	}
	return
}

func tryFillAuditModify(ctx context.Context, rv reflect.Value, st *ModelStructure) (err error) {
	_, _, amb, amt, _, _, _, hasAudits := st.AuditFields()
	if !hasAudits {
		return
	}
	if amb != nil {
		err = tryFillByField(ctx, rv, amb)
		if err != nil {
			return
		}
	}
	if amt != nil {
		err = tryFillATField(rv, amt)
		if err != nil {
			return
		}
	}
	return
}

func tryFillAuditDelete(ctx context.Context, rv reflect.Value, st *ModelStructure) (err error) {
	_, _, _, _, adb, adt, _, hasAudits := st.AuditFields()
	if !hasAudits {
		return
	}
	if adb != nil {
		err = tryFillByField(ctx, rv, adb)
		if err != nil {
			return
		}
	}
	if adt != nil {
		err = tryFillATField(rv, adt)
		if err != nil {
			return
		}
	}
	return
}
