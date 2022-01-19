package postgres

import (
	"fmt"
	"github.com/aacfactory/fns"
	"reflect"
	"strconv"
	"time"
)

func tryFillAuditCreate(ctx fns.Context, rv reflect.Value, tab *table) (err error) {
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
			createByField := rv.FieldByName(createByColumn.FieldName)
			if createByField.Type().Kind() == reflect.String {
				createByStringTypeKind = true
			}
			createBY = createByField.Interface()
		}
		if create.isAct() {
			createAtColumn = create
			createAT = rv.FieldByName(createAtColumn.FieldName).Convert(reflect.TypeOf(createAT)).Interface().(time.Time)
		}
	}
	if createByColumn != nil {
		hasCreateByValue := false
		if createByStringTypeKind {
			createByString := createBY.(string)
			if createByString == "" {
				user := ctx.User()
				if !user.Exists() && user.Id() != "" {
					createByString = user.Id()
					rv.FieldByName(createByColumn.FieldName).SetString(createByString)
					hasCreateByValue = true
				}
			} else {
				hasCreateByValue = true
			}
		} else {
			createByInt := reflect.ValueOf(createBY).Int()
			if createByInt <= 0 {
				user := ctx.User()
				if !user.Exists() && user.Id() != "" {
					createByString := user.Id()
					createByInt0, toIntErr := strconv.Atoi(createByString)
					if toIntErr != nil {
						err = fmt.Errorf("create by type is int but type of user id in context is not int")
						return
					}
					createByInt = int64(createByInt0)
					rv.FieldByName(createByColumn.FieldName).SetInt(createByInt)
					hasCreateByValue = true
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
			createAtField := rv.FieldByName(createAtColumn.FieldName)
			createAtField.Set(reflect.ValueOf(createAT).Convert(createAtField.Type()))
		}
	}
	return
}

func tryFillAuditModify(ctx fns.Context, rv reflect.Value, tab *table) (err error) {
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
			modifyByField := rv.FieldByName(modifyByColumn.FieldName)
			if modifyByField.Type().Kind() == reflect.String {
				modifyByStringTypeKind = true
			}
			modifyBY = modifyByField.Interface()
		}
		if modify.isAmt() {
			modifyAtColumn = modify
			modifyAT = rv.FieldByName(modifyAtColumn.FieldName).Convert(reflect.TypeOf(modifyAT)).Interface().(time.Time)
		}
	}
	if modifyByColumn != nil {
		hasModifyByValue := false
		if modifyByStringTypeKind {
			modifyByString := modifyBY.(string)
			if modifyByString == "" {
				user := ctx.User()
				if !user.Exists() && user.Id() != "" {
					modifyByString = user.Id()
					rv.FieldByName(modifyByColumn.FieldName).SetString(modifyByString)
					hasModifyByValue = true
				}
			} else {
				hasModifyByValue = true
			}
		} else {
			modifyByInt := reflect.ValueOf(modifyBY).Int()
			if modifyByInt <= 0 {
				user := ctx.User()
				if !user.Exists() && user.Id() != "" {
					modifyByString := user.Id()
					modifyByInt0, toIntErr := strconv.Atoi(modifyByString)
					if toIntErr != nil {
						err = fmt.Errorf("modify by type is int but type of user id in context is not int")
						return
					}
					modifyByInt = int64(modifyByInt0)
					rv.FieldByName(modifyByColumn.FieldName).SetInt(modifyByInt)
					hasModifyByValue = true
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
			modifyAtField := rv.FieldByName(modifyAtColumn.FieldName)
			modifyAtField.Set(reflect.ValueOf(modifyAT).Convert(modifyAtField.Type()))
		}
	}
	return
}

func tryFillAuditVersion(rv reflect.Value, tab *table) {
	versionColumn := tab.findAuditVersion()
	if versionColumn != nil {
		field := rv.FieldByName(versionColumn.FieldName)
		field.SetInt(field.Int() + 1)
	}
	return
}
