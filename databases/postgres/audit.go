package postgres

import (
	"context"
	"fmt"
	"github.com/aacfactory/fns/service"
	"reflect"
	"time"
)

func tryFillAuditCreate(ctx context.Context, rv reflect.Value, tab *table) (err error) {
	creates := tab.findAuditCreate()
	if len(creates) == 0 {
		return
	}
	var createBY interface{}
	createByStringTypeKind := false
	var createByColumn *column
	createAT := time.Time{}
	var createAtColumn *column
	for _, create := range creates {
		if create.isAcb() {
			createByColumn = create
			createByField := rv.Elem().FieldByName(createByColumn.FieldName)
			if createByField.Type().Kind() == reflect.String {
				createByStringTypeKind = true
			}
			createBY = createByField.Interface()
		}
		if create.isAct() {
			createAtColumn = create
			createAT = rv.Elem().FieldByName(createAtColumn.FieldName).Convert(reflect.TypeOf(createAT)).Interface().(time.Time)
		}
	}
	if createByColumn != nil {
		hasCreateByValue := false
		if createByStringTypeKind {
			createByString := createBY.(string)
			if createByString == "" {
				request, hasRequest := service.GetRequest(ctx)
				if hasRequest {
					userId := request.User().Id()
					if userId != "" {
						createByString = userId
						rv.Elem().FieldByName(createByColumn.FieldName).SetString(createByString)
						hasCreateByValue = true
					}
				}
			} else {
				hasCreateByValue = true
			}
		} else {
			createByInt := reflect.ValueOf(createBY).Int()
			if createByInt <= 0 {
				request, hasRequest := service.GetRequest(ctx)
				if hasRequest {
					userId := request.User().Id()
					if userId != "" {
						createByInt = request.User().IntId()
						rv.Elem().FieldByName(createByColumn.FieldName).SetInt(createByInt)
						hasCreateByValue = true
					}
				}
			} else {
				hasCreateByValue = true
			}
		}
		if !hasCreateByValue {
			err = fmt.Errorf("create by column value is needed")
			return
		}
	}
	if createAtColumn != nil {
		if createAT.IsZero() {
			createAT = time.Now()
			createAtField := rv.Elem().FieldByName(createAtColumn.FieldName)
			createAtField.Set(reflect.ValueOf(createAT).Convert(createAtField.Type()))
		}
	}
	return
}

func tryFillAuditModify(ctx context.Context, rv reflect.Value, tab *table) (err error) {
	modifies := tab.findAuditModify()
	if len(modifies) == 0 {
		return
	}
	var modifyBY interface{}
	modifyByStringTypeKind := false
	var modifyByColumn *column
	modifyAT := time.Time{}
	var modifyAtColumn *column
	for _, modify := range modifies {
		if modify.isAmb() {
			modifyByColumn = modify
			modifyByField := rv.Elem().FieldByName(modifyByColumn.FieldName)
			if modifyByField.Type().Kind() == reflect.String {
				modifyByStringTypeKind = true
			}
			modifyBY = modifyByField.Interface()
		}
		if modify.isAmt() {
			modifyAtColumn = modify
			modifyAT = rv.Elem().FieldByName(modifyAtColumn.FieldName).Convert(reflect.TypeOf(modifyAT)).Interface().(time.Time)
		}
	}
	if modifyByColumn != nil {
		hasModifyByValue := false
		if modifyByStringTypeKind {
			modifyByString := modifyBY.(string)
			if modifyByString == "" {
				request, hasRequest := service.GetRequest(ctx)
				if hasRequest {
					userId := request.User().Id()
					if userId != "" {
						modifyByString = request.User().Id()
						rv.Elem().FieldByName(modifyByColumn.FieldName).SetString(modifyByString)
						hasModifyByValue = true
					}
				}
			} else {
				hasModifyByValue = true
			}
		} else {
			modifyByInt := reflect.ValueOf(modifyBY).Int()
			if modifyByInt <= 0 {
				request, hasRequest := service.GetRequest(ctx)
				if hasRequest {
					userId := request.User().Id()
					if userId != "" {
						modifyByInt = request.User().IntId()
						rv.Elem().FieldByName(modifyByColumn.FieldName).SetInt(modifyByInt)
						hasModifyByValue = true
					}
				}
			} else {
				hasModifyByValue = true
			}
		}
		if !hasModifyByValue {
			err = fmt.Errorf("modify by column value is needed")
			return
		}
	}
	if modifyAtColumn != nil {
		if modifyAT.IsZero() {
			modifyAT = time.Now()
			modifyAtField := rv.Elem().FieldByName(modifyAtColumn.FieldName)
			modifyAtField.Set(reflect.ValueOf(modifyAT).Convert(modifyAtField.Type()))
		}
	}
	return
}

func tryFillAuditDelete(ctx context.Context, rv reflect.Value, tab *table) (err error) {
	deletes := tab.findAuditDelete()
	if len(deletes) == 0 {
		return
	}
	var deleteBY interface{}
	deleteByStringTypeKind := false
	var deleteByColumn *column
	deleteAT := time.Time{}
	var deleteAtColumn *column
	for _, delete0 := range deletes {
		if delete0.isAdb() {
			deleteByColumn = delete0
			deleteByField := rv.Elem().FieldByName(deleteByColumn.FieldName)
			if deleteByField.Type().Kind() == reflect.String {
				deleteByStringTypeKind = true
			}
			deleteBY = deleteByField.Interface()
		}
		if delete0.isAdt() {
			deleteAtColumn = delete0
			deleteAT = rv.Elem().FieldByName(deleteAtColumn.FieldName).Convert(reflect.TypeOf(deleteAT)).Interface().(time.Time)
		}
	}
	if deleteByColumn != nil {
		hasDeleteByValue := false
		if deleteByStringTypeKind {
			deleteByString := deleteBY.(string)
			if deleteByString == "" {
				request, hasRequest := service.GetRequest(ctx)
				if hasRequest {
					userId := request.User().Id()
					if userId != "" {
						deleteByString = request.User().Id()
						rv.Elem().FieldByName(deleteByColumn.FieldName).SetString(deleteByString)
						hasDeleteByValue = true
					}
				}
			} else {
				hasDeleteByValue = true
			}
		} else {
			deleteByInt := reflect.ValueOf(deleteBY).Int()
			if deleteByInt <= 0 {
				request, hasRequest := service.GetRequest(ctx)
				if hasRequest {
					userId := request.User().Id()
					if userId != "" {
						deleteByInt = request.User().IntId()
						rv.Elem().FieldByName(deleteByColumn.FieldName).SetInt(deleteByInt)
						hasDeleteByValue = true
					}
				}
			} else {
				hasDeleteByValue = true
			}
		}
		if !hasDeleteByValue {
			err = fmt.Errorf("delete by column value is needed")
			return
		}
	}
	if deleteAtColumn != nil {
		if deleteAT.IsZero() {
			deleteAT = time.Now()
			deleteAtField := rv.Elem().FieldByName(deleteAtColumn.FieldName)
			deleteAtField.Set(reflect.ValueOf(deleteAT).Convert(deleteAtField.Type()))
		}
	}
	return
}

func tryFillAuditVersion(rv reflect.Value, tab *table) {
	versionColumn := tab.findAuditVersion()
	if versionColumn != nil {
		field := rv.Elem().FieldByName(versionColumn.FieldName)
		field.SetInt(field.Int() + 1)
	}
	return
}

func tryFillAuditVersionExact(rv reflect.Value, tab *table, v int64) {
	versionColumn := tab.findAuditVersion()
	if versionColumn != nil {
		field := rv.Elem().FieldByName(versionColumn.FieldName)
		field.SetInt(v)
	}
	return
}
