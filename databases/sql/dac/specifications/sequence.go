package specifications

import (
	"fmt"
	"github.com/aacfactory/errors"
	"reflect"
	"strings"
)

type SequenceInfo struct {
	Name    string
	Schema  string
	Options []string
}

func GetSequenceInfo(e any) (info SequenceInfo, err error) {
	rv := reflect.Indirect(reflect.ValueOf(e))
	rt := rv.Type()
	// info
	_, hasInfoFunc := rt.MethodByName("SequenceInfo")
	if !hasInfoFunc {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has not SequenceInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	infoFunc := rv.MethodByName("SequenceInfo")
	results := infoFunc.Call(nil)
	if len(results) != 1 {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has invalid SequenceInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	result := results[0]
	// name
	_, hasNameFunc := result.Type().MethodByName("Name")
	if !hasNameFunc {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has not SequenceInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	nameResults := result.MethodByName("Name").Call(nil)
	if len(nameResults) != 1 && nameResults[0].Type().Kind() != reflect.String {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has invalid SequenceInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	name := nameResults[0].String()
	// schema
	_, hasSchemaFunc := result.Type().MethodByName("Schema")
	if !hasSchemaFunc {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has not SequenceInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	schemaResults := result.MethodByName("Schema").Call(nil)
	if len(schemaResults) != 1 && schemaResults[0].Type().Kind() != reflect.String {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has invalid SequenceInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	schema := schemaResults[0].String()
	// Options
	_, hasOptionsFunc := result.Type().MethodByName("Options")
	if !hasOptionsFunc {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has not SequenceInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	optionsResults := result.MethodByName("Options").Call(nil)
	if len(optionsResults) != 1 && optionsResults[0].Type().Kind() != reflect.Slice && optionsResults[0].Type().Elem().Kind() != reflect.String {
		err = errors.Warning(fmt.Sprintf("sql: %s.%s has invalid SequenceInfo func", rt.PkgPath(), rt.Name()))
		return
	}
	options := optionsResults[0].Interface().([]string)
	for i, option := range options {
		options[i] = strings.TrimSpace(option)
	}
	// view
	info = SequenceInfo{
		Schema:  strings.TrimSpace(schema),
		Name:    strings.TrimSpace(name),
		Options: options,
	}
	return
}

func GetSequence(sequence any) (info *SequenceInfo, err error) {
	rt := reflect.TypeOf(sequence)
	key := fmt.Sprintf("%s.%s", rt.PkgPath(), rt.Name())
	scanned, has := sequences.Load(key)
	if has {
		info, has = scanned.(*SequenceInfo)
		if !has {
			err = errors.Warning("sql: get sequence specification failed").WithCause(fmt.Errorf("stored sequence specification is invalid type"))
			return
		}
		return
	}
	scanned, err, _ = group.Do(key, func() (v interface{}, err error) {
		v, err = GetSequenceInfo(sequence)
		if err != nil {
			return
		}
		sequences.Store(key, &v)
		return
	})
	if err != nil {
		err = errors.Warning("sql: get sequence specification failed").WithCause(err)
		return
	}
	info = scanned.(*SequenceInfo)
	return
}
