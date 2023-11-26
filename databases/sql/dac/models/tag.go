package models

import (
	"reflect"
	"strings"
)

const (
	columnTag = "column"
	jsonTag   = "json"
)

const (
	pkColumn        = "pk"
	incrColumn      = "incr"
	jsonColumn      = "json"
	acbColumn       = "acb"
	actColumn       = "act"
	ambColumn       = "amb"
	amtColumn       = "amt"
	adbColumn       = "adb"
	adtColumn       = "adt"
	aolColumn       = "version"
	virtualColumn   = "vc"
	referenceColumn = "ref"
	linkColumn      = "link"
	linksColumn     = "links"
	treeColumn      = "tree"
)

func getColumn(rt reflect.StructField) (name string, options string, ok bool) {
	name, ok = rt.Tag.Lookup(columnTag)
	if !ok {
		return
	}
	name = strings.TrimSpace(name)
	idx := strings.IndexByte(name, ',')
	if idx > -1 {
		options = strings.TrimSpace(name[idx+1:])
		name = name[0:idx]
	}
	return
}
