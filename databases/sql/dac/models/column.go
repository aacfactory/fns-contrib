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
	aolColumn       = "aol"
	virtualColumn   = "vc"
	referenceColumn = "ref"
	linkColumn      = "link"
	linksColumn     = "links"
	treeColumn      = "tree"
)

const (
	Normal ColumnKind = iota
	Pk
	Acb
	Act
	Amb
	Amt
	Adb
	Adt
	Aol
	Json
	Virtual
	Reference
	Link
	Links
	Tree
)

type ColumnKind int

// Column
// 'column:"{name},{kind},{options}"'
type Column struct {
	Field   string
	Name    string
	Kind    ColumnKind
	Options []string
}

func (column *Column) Incr() bool {
	if len(column.Options) > 0 {
		return column.Options[0] == incrColumn
	}
	return false
}

func (column *Column) Virtual() (query string) {
	query = column.Options[0]
	return
}

func (column *Column) Reference() (host string, target string) {
	host = column.Options[0]
	target = column.Options[1]
	return
}

func (column *Column) Link() (host string, target string) {
	host = column.Options[0]
	target = column.Options[1]
	return
}

func (column *Column) Links() (host string, target string) {
	host = column.Options[0]
	target = column.Options[1]
	return
}

func NewColumn(rt reflect.StructField) (column Column, ok bool) {
	tag, hasTag := rt.Tag.Lookup(columnTag)
	if !hasTag {
		return
	}
	name := strings.TrimSpace(tag)
	kind := Normal
	options := make([]string, 0, 1)
	idx := strings.IndexByte(tag, ',')
	if idx > -1 {
		name = tag[0:idx]
		tag = strings.TrimSpace(tag[idx+1:])
		idx = strings.IndexByte(tag, ',')
		if idx > 0 {
			kv := strings.ToLower(tag[0:idx])
			if len(tag) > idx {
				tag = strings.TrimSpace(tag[idx+1:])
			}
			switch kv {
			case pkColumn:
				kind = Pk
				if strings.ToLower(tag) == incrColumn {
					options = append(options, incrColumn)
				}
				break
			case acbColumn:
				kind = Acb
				break
			case actColumn:
				kind = Act
				break
			case ambColumn:
				kind = Amb
				break
			case amtColumn:
				kind = Amt
				break
			case adbColumn:
				kind = Adb
				break
			case adtColumn:
				kind = Adt
				break
			case aolColumn:
				kind = Aol
				break
			case jsonColumn:
				kind = Json
				break
			case virtualColumn:
				// name,vc,{query}
				kind = Virtual
				options = append(options, tag)
				break
			case referenceColumn:
				// name,ref,self+target
				kind = Reference
				items := strings.Split(tag, "+")
				for _, item := range items {
					options = append(options, strings.TrimSpace(item))
				}
				break
			case linkColumn:
				// name,link,self+target
				kind = Link
				items := strings.Split(tag, "+")
				for _, item := range items {
					options = append(options, strings.TrimSpace(item))
				}
				break
			case linksColumn:
				// name,links,self+target
				kind = Links
				items := strings.Split(tag, "+")
				for _, item := range items {
					options = append(options, strings.TrimSpace(item))
				}
				break
			case treeColumn:
				// name,tree,self+parent
				kind = Tree
				items := strings.Split(tag, "+")
				for _, item := range items {
					options = append(options, strings.TrimSpace(item))
				}
				break
			default:
				kind = Normal
				break
			}
		}
	}
	column = Column{
		Field:   rt.Name,
		Name:    name,
		Kind:    kind,
		Options: options,
	}
	ok = true
	return
}
