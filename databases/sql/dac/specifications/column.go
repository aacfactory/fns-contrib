package specifications

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/orders"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	columnTag       = "column"
	discardTagValue = "-"
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
)

const (
	UnknownVirtualQueryKind VirtualQueryKind = iota
	BasicVirtualQuery
	ObjectVirtualQuery
	ArrayVirtualQuery
)

type VirtualQueryKind int

const (
	Normal    ColumnKind = iota // column
	Pk                          // column,pk{,incr}
	Acb                         // column,acb
	Act                         // column,act
	Amb                         // column,amb
	Amt                         // column,amt
	Adb                         // column,adb
	Adt                         // column,adt
	Aol                         // column,aol
	Json                        // column,json
	Virtual                     // ident,vc,basic|object|array,query
	Reference                   // column,ref,field+target_field
	Link                        // ident,link,field+target_field
	Links                       // column,links,field+target_field,orders:field@desc+field,length:10
)

type ColumnKind int

const (
	UnknownType ColumnTypeName = iota
	StringType
	BoolType
	IntType
	FloatType
	DatetimeType
	DateType
	TimeType
	BytesType
	ByteType
	JsonType
	ScanType
	MappingType
)

type ColumnTypeName int

type ColumnType struct {
	Name    ColumnTypeName
	Value   reflect.Type
	Mapping *Specification
	Options []string
}

func (ct *ColumnType) fillName() {
	if ct.Name == UnknownType {
		if ct.Value.ConvertibleTo(stringType) || ct.Value.ConvertibleTo(nullStringType) {
			ct.Name = StringType
		} else if ct.Value.ConvertibleTo(boolType) || ct.Value.ConvertibleTo(nullBoolType) {
			ct.Name = BoolType
		} else if ct.Value.ConvertibleTo(intType) || ct.Value.ConvertibleTo(nullInt16Type) || ct.Value.ConvertibleTo(nullInt32Type) || ct.Value.ConvertibleTo(nullInt64Type) {
			ct.Name = IntType
		} else if ct.Value.ConvertibleTo(floatType) || ct.Value.ConvertibleTo(nullFloatType) {
			ct.Name = FloatType
		} else if ct.Value.ConvertibleTo(datetimeType) || ct.Value.ConvertibleTo(nullTimeType) {
			ct.Name = DatetimeType
		} else if ct.Value.ConvertibleTo(dateType) {
			ct.Name = DateType
		} else if ct.Value.ConvertibleTo(timeType) {
			ct.Name = TimeType
		} else if ct.Value.ConvertibleTo(bytesType) {
			ct.Name = BytesType
		} else if ct.Value.ConvertibleTo(byteType) || ct.Value.ConvertibleTo(nullByteType) {
			ct.Name = ByteType
		} else if ct.Value.ConvertibleTo(rawType) {
			ct.Name = BytesType
		} else if ct.Value.ConvertibleTo(scannerType) && ct.Value.ConvertibleTo(jsonMarshalerType) {
			ct.Name = ScanType
		} else {
			return
		}
	}
}

// Column
// 'column:"{name},{kind},{options}"'
type Column struct {
	Field    string
	FieldIdx int
	Name     string
	Kind     ColumnKind
	Type     ColumnType
}

func (column *Column) Incr() bool {
	if len(column.Type.Options) > 0 {
		return column.Type.Options[0] == incrColumn
	}
	return false
}

func (column *Column) Virtual() (kind VirtualQueryKind, query string, ok bool) {
	if column.Kind == Virtual {
		switch column.Type.Options[0] {
		case "basic":
			kind = BasicVirtualQuery
		case "object":
			kind = ObjectVirtualQuery
		case "array":
			kind = ArrayVirtualQuery
			break
		default:
			kind = UnknownVirtualQueryKind
			break
		}
		query = column.Type.Options[1]
		ok = true
	}
	return
}

func (column *Column) Reference() (hostField string, awayField string, mapping *Specification, ok bool) {
	ok = column.Kind == Reference
	if ok {
		hostField = column.Type.Options[0]
		awayField = column.Type.Options[1]
		mapping = column.Type.Mapping
	}
	return
}

func (column *Column) Link() (host string, target string, mapping *Specification, ok bool) {
	ok = column.Kind == Link
	if ok {
		host = column.Type.Options[0]
		target = column.Type.Options[1]
		mapping = column.Type.Mapping
	}
	return
}

func (column *Column) Links() (host string, target string, mapping *Specification, order orders.Orders, length int, ok bool) {
	ok = column.Kind == Links
	if ok {
		host = column.Type.Options[0]
		target = column.Type.Options[1]
		mapping = column.Type.Mapping
		if optLen := len(column.Type.Options); optLen > 2 {
			for i := 2; i < optLen; i++ {
				option := strings.TrimSpace(column.Type.Options[i])
				// orders:
				if idx := strings.Index(strings.ToLower(option), "orders:"); idx == 0 {
					option = option[7:]
					items := strings.Split(option, "+")
					for _, item := range items {
						item = strings.TrimSpace(item)
						pos := strings.IndexByte(item, '@')
						if pos == -1 {
							order = orders.Asc(item)
						} else {
							field := strings.TrimSpace(item[0:pos])
							kind := strings.ToLower(strings.TrimSpace(item[pos+1:]))
							if kind == "desc" {
								order = orders.Desc(field)
							} else {
								order = orders.Asc(field)
							}
						}
					}
				}
				// length:
				if idx := strings.Index(strings.ToLower(option), "length:"); idx == 0 {
					option = strings.TrimSpace(option[7:])
					length, _ = strconv.Atoi(option)
				}
			}
		}
	}
	return
}

func (column *Column) Valid() bool {
	if column.Type.Name != UnknownType {
		return false
	}
	if column.Incr() {
		return column.Type.Name == IntType
	}
	ok := false
	switch column.Kind {
	case Acb, Amb, Adb:
		ok = column.Type.Name == IntType || column.Type.Name == StringType
		break
	case Act, Amt, Adt:
		ok = column.Type.Name == DatetimeType || column.Type.Name == IntType
		break
	case Aol:
		ok = column.Type.Name == IntType
		break
	case Reference, Link:
		ok = column.Type.Value.Kind() == reflect.Struct ||
			(column.Type.Value.Kind() == reflect.Ptr && column.Type.Value.Elem().Kind() == reflect.Struct)
		break
	case Links:
		ok = column.Type.Value.Kind() == reflect.Slice &&
			(column.Type.Value.Elem().Kind() == reflect.Struct ||
				(column.Type.Value.Elem().Kind() == reflect.Ptr && column.Type.Value.Elem().Elem().Kind() == reflect.Struct))
		break
	default:

		break
	}
	return ok
}

func (column *Column) ZeroValue() (v any) {
	switch column.Type.Value.Kind() {
	case reflect.Ptr:
		v = reflect.New(column.Type.Value.Elem()).Elem().Interface()
		break
	default:
		v = reflect.New(column.Type.Value).Elem().Interface()
		break
	}
	return
}

func (column *Column) ScanValue() (v interface{}, err error) {
	if column.Type.Value.Implements(scannerType) {
		typ := column.Type.Value
		if typ.Kind() == reflect.Ptr {
			typ = column.Type.Value.Elem()
		}
		v = reflect.Indirect(reflect.New(typ)).Interface()
		return
	}
	switch column.Type.Name {
	case StringType:
		v = ""
		break
	case BoolType:
		v = false
		break
	case IntType:
		v = int64(0)
		break
	case FloatType:
		v = float64(0)
		break
	case DatetimeType:
		v = time.Time{}
		break
	case DateType:
		v = DateValue{}
		break
	case TimeType:
		v = TimeValue{}
		break
	case ByteType:
		v = byte(0)
		break
	case BytesType:
		v = []byte{}
		break
	case JsonType:
		v = JsonValue{}
		break
	case MappingType:
		v = JsonValue{}
		break
	default:
		err = errors.Warning("sql: type of column can not be scanned").WithMeta("column", column.Name)
		return
	}
	return
}

func newColumn(ctx context.Context, ri int, rt reflect.StructField) (column *Column, err error) {
	tag, hasTag := rt.Tag.Lookup(columnTag)
	if !hasTag {
		return
	}
	tag = strings.TrimSpace(tag)
	if tag == discardTagValue {
		return
	}
	kind := Normal
	typ := ColumnType{
		Name:    UnknownType,
		Value:   rt.Type,
		Mapping: nil,
		Options: make([]string, 0, 1),
	}
	items := strings.Split(tag, ",")

	name := strings.TrimSpace(items[0])
	if len(items) > 1 {
		items = items[1:]
		kv := strings.ToLower(strings.TrimSpace(items[0]))
		switch kv {
		case pkColumn:
			kind = Pk
			if strings.ToLower(tag) == incrColumn {
				typ.Options = append(typ.Options, incrColumn)
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
			typ.Name = JsonType
			break
		case virtualColumn:
			// name,vc,{kind},{query}
			if len(items) < 3 {
				err = errors.Warning("sql: scan virtual column failed, kind and query are required").WithMeta("field", rt.Name)
				return
			}
			kind = Virtual
			vck := strings.ToLower(strings.TrimSpace(items[1]))
			valid := vck == "basic" || vck == "object" || vck == "array"
			if !valid {
				err = errors.Warning("sql: scan virtual column failed, kind is invalid").WithMeta("field", rt.Name)
				return
			}
			typ.Options = append(typ.Options, vck, strings.TrimSpace(items[2]))
			if vck != "basic" {
				typ.Name = JsonType
			}
			break
		case referenceColumn:
			// name,ref,self+target
			if len(items) < 2 {
				err = errors.Warning("sql: scan reference column failed, mapping is required").WithMeta("field", rt.Name)
				return
			}
			mr := strings.Split(items[1], "+")
			if len(mr) != 2 {
				err = errors.Warning("sql: scan reference column failed, mapping is invalid").WithMeta("field", rt.Name)
				return
			}

			kind = Reference
			typ.Options = append(typ.Options, strings.TrimSpace(mr[0]))
			typ.Options = append(typ.Options, strings.TrimSpace(mr[1]))
			typ.Name = MappingType
			switch rt.Type.Kind() {
			case reflect.Struct:
				typ.Mapping, err = GetSpecification(ctx, reflect.Zero(rt.Type))
				if err != nil {
					err = errors.Warning("sql: scan reference column failed").WithMeta("field", rt.Name).WithCause(fmt.Errorf("reference column type must be implement Table")).WithCause(err)
					return
				}
				break
			case reflect.Ptr:
				typ.Mapping, err = GetSpecification(ctx, reflect.Zero(rt.Type.Elem()))
				if err != nil {
					err = errors.Warning("sql: scan reference column failed").WithMeta("field", rt.Name).WithCause(fmt.Errorf("reference column type must be implement Table")).WithCause(err)
					return
				}
				break
			default:
				err = errors.Warning("sql: scan reference column failed").WithMeta("field", rt.Name).WithCause(fmt.Errorf("reference column type must be struct or ptr struct"))
				return
			}
			break
		case linkColumn:
			// name,link,self+target
			if len(items) < 2 {
				err = errors.Warning("sql: scan link column failed, mapping is required").WithMeta("field", rt.Name)
				return
			}
			mr := strings.Split(items[1], "+")
			if len(mr) != 2 {
				err = errors.Warning("sql: scan link column failed, mapping is invalid").WithMeta("field", rt.Name)
				return
			}

			kind = Link
			typ.Options = append(typ.Options, strings.TrimSpace(mr[0]))
			typ.Options = append(typ.Options, strings.TrimSpace(mr[1]))
			typ.Name = MappingType
			switch rt.Type.Kind() {
			case reflect.Struct:
				typ.Mapping, err = GetSpecification(ctx, reflect.Zero(rt.Type))
				if err != nil {
					err = errors.Warning("sql: scan link column failed").WithMeta("field", rt.Name).WithCause(fmt.Errorf("link column type must be implement Table")).WithCause(err)
					return
				}
				break
			case reflect.Ptr:
				typ.Mapping, err = GetSpecification(ctx, reflect.Zero(rt.Type.Elem()))
				if err != nil {
					err = errors.Warning("sql: scan link column failed").WithMeta("field", rt.Name).WithCause(fmt.Errorf("link column type must be implement Table")).WithCause(err)
					return
				}
				break
			default:
				err = errors.Warning("sql: scan link column failed").WithMeta("field", rt.Name).WithCause(fmt.Errorf("link column type must be struct or ptr struct"))
				return
			}
			break
		case linksColumn:
			// name,links,self+target
			if len(items) < 2 {
				err = errors.Warning("sql: scan links column failed, mapping is required").WithMeta("field", rt.Name)
				return
			}
			mr := strings.Split(items[1], "+")
			if len(mr) != 2 {
				err = errors.Warning("sql: scan links column failed, mapping is invalid").WithMeta("field", rt.Name)
				return
			}

			kind = Links
			typ.Options = append(typ.Options, strings.TrimSpace(mr[0]))
			typ.Options = append(typ.Options, strings.TrimSpace(mr[1]))
			if len(items) > 2 {
				typ.Options = append(typ.Options, items[2:]...)
			}

			typ.Name = MappingType
			if rt.Type.Kind() != reflect.Slice {
				err = errors.Warning("sql: scan links column failed").WithMeta("field", rt.Name).WithCause(fmt.Errorf("links column type must be slice struct or slice ptr struct"))
				return
			}
			switch rt.Type.Elem().Kind() {
			case reflect.Struct:
				typ.Mapping, err = GetSpecification(ctx, reflect.Zero(rt.Type.Elem()))
				if err != nil {
					err = errors.Warning("sql: scan links column failed").WithMeta("field", rt.Name).WithCause(fmt.Errorf("links column type must be implement Table")).WithCause(err)
					return
				}
				break
			case reflect.Ptr:
				typ.Mapping, err = GetSpecification(ctx, reflect.Zero(rt.Type.Elem().Elem()))
				if err != nil {
					err = errors.Warning("sql: scan links column failed").WithMeta("field", rt.Name).WithCause(fmt.Errorf("links column type must be implement Table")).WithCause(err)
					return
				}
				break
			default:
				err = errors.Warning("sql: scan links column failed").WithMeta("field", rt.Name).WithCause(fmt.Errorf("links column type must be struct or ptr struct"))
				return
			}
			break
		default:
			kind = Normal
			break
		}
	}

	typ.fillName()

	column = &Column{
		Field:    rt.Name,
		FieldIdx: ri,
		Name:     name,
		Kind:     kind,
		Type:     typ,
	}

	if !column.Valid() {
		err = errors.Warning("sql: new column failed").WithMeta("field", rt.Name).WithCause(fmt.Errorf("%v is not supported", typ.Value))
		return
	}
	return
}
