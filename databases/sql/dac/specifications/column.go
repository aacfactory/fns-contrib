package specifications

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/orders"
	"reflect"
	"strconv"
	"strings"
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
	AggregateVirtualQuery
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
	Virtual                     // ident,vc,basic|object|array|aggregate,query|agg_func
	Reference                   // column,ref,target_field
	Link                        // ident,link,field+target_field
	Links                       // column,links,field+target_field,orders:field@desc+field,length:10
)

type ColumnKind int

func (kind ColumnKind) String() string {
	switch kind {
	case Normal:
		return "normal"
	case Pk:
		return "pk"
	case Acb:
		return "acb"
	case Act:
		return "act"
	case Amb:
		return "amb"
	case Amt:
		return "amt"
	case Adb:
		return "adb"
	case Adt:
		return "adt"
	case Aol:
		return "aol"
	case Json:
		return "json"
	case Virtual:
		return "virtual"
	case Reference:
		return "reference"
	case Link:
		return "link"
	case Links:
		return "links"
	}
	return "???"
}

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

func (ct *ColumnType) String() string {
	switch ct.Name {
	case UnknownType:
		return "unknown"
	case StringType:
		return "string"
	case BoolType:
		return "bool"
	case IntType:
		return "int"
	case FloatType:
		return "float"
	case DatetimeType:
		return "datetime"
	case DateType:
		return "date"
	case TimeType:
		return "time"
	case ByteType:
		return "byte"
	case BytesType:
		return "bytes"
	case JsonType:
		return "json"
	case ScanType:
		return "scan"
	case MappingType:
		return fmt.Sprintf("mapping(%s, %s)", ct.Mapping.Key, fmt.Sprintf("%+v", ct.Options))
	}
	return "???"
}

// Column
// 'column:"{name},{kind},{options}"'
type Column struct {
	FieldIdx    []int
	Field       string
	Name        string
	JsonIdent   string
	Kind        ColumnKind
	Type        ColumnType
	ValueWriter ValueWriter
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
		case "agg", "aggregate":
			kind = AggregateVirtualQuery
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

func (column *Column) Reference() (awayField string, mapping *Specification, ok bool) {
	ok = column.Kind == Reference
	if ok {
		awayField = column.Type.Options[0]
		mapping = column.Type.Mapping
	}
	return
}

func (column *Column) Link() (hostField string, awayField string, mapping *Specification, ok bool) {
	ok = column.Kind == Link
	if ok {
		hostField = column.Type.Options[0]
		awayField = column.Type.Options[1]
		mapping = column.Type.Mapping
	}
	return
}

func (column *Column) Links() (hostField string, awayField string, mapping *Specification, order orders.Orders, length int, ok bool) {
	ok = column.Kind == Links
	if ok {
		hostField = column.Type.Options[0]
		awayField = column.Type.Options[1]
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
	if column.Type.Name == UnknownType {
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
		if column.Incr() {
			ok = column.Type.Name == IntType
			break
		}
		ok = true
		break
	}
	return ok
}

func (column *Column) String() (s string) {
	kind := column.Kind.String()
	if column.Kind == Virtual {
		kind = kind + fmt.Sprintf("(%+v)", column.Type.Options)
	} else if column.Kind == Json {
		if column.Type.Value.ConvertibleTo(bytesType) {
			kind = kind + "(bytes)"
		} else {
			kind = kind + fmt.Sprintf("(%s)", column.Type.Value.String())
		}
	}
	s = fmt.Sprintf(
		"%s => field:%s, kind: %s, type: %s",
		column.Name,
		column.Field,
		kind, column.Type.String(),
	)
	return
}

func (column *Column) WriteValue(field reflect.Value, value any) (err error) {
	err = column.ValueWriter.Write(value, field)
	return
}

func (column *Column) ReadValue(sv reflect.Value) (fv reflect.Value) {
	for i := len(column.FieldIdx) - 1; i > -1; i-- {
		sv = sv.Field(column.FieldIdx[i])
	}
	fv = sv
	return
}

func newColumn(ctx context.Context, rt reflect.StructField, idx []int) (column *Column, err error) {
	tag, hasTag := rt.Tag.Lookup(columnTag)
	if !hasTag {
		return
	}
	tag = strings.TrimSpace(tag)
	if tag == discardTagValue {
		return
	}
	var vw ValueWriter
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
			if len(items) > 1 {
				if strings.ToLower(items[1]) == incrColumn {
					switch rt.Type.Kind() {
					case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						vw = &IntValue{}
						break
					default:
						err = errors.Warning("sql: type of incr pk column failed must be int64").WithMeta("field", rt.Name)
						return
					}
					typ.Name = IntType
					typ.Options = append(typ.Options, incrColumn)
				}
			}
			if typ.Name == UnknownType {
				switch rt.Type.Kind() {
				case reflect.String:
					typ.Name = StringType
					vw = &StringValue{}
					break
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					typ.Name = IntType
					vw = &IntValue{}
					break
				default:
					err = errors.Warning("sql: type of pk column failed must be int64 or string").WithMeta("field", rt.Name)
					return
				}
			}
			break
		case acbColumn:
			kind = Acb
			vw, typ.Name, err = NewBasicValueWriter(rt.Type)
			if err != nil {
				err = errors.Warning("sql: type of acb column failed must be int64 or string").WithCause(err).WithMeta("field", rt.Name)
				return
			}
			if typ.Name != StringType && typ.Name != IntType {
				err = errors.Warning("sql: type of acb column failed must be int64 or string").WithMeta("field", rt.Name)
				return
			}
			break
		case actColumn:
			kind = Act
			vw, typ.Name, err = NewBasicValueWriter(rt.Type)
			if err != nil {
				err = errors.Warning("sql: type of act column failed must be time.Time or int64").WithCause(err).WithMeta("field", rt.Name)
				return
			}
			if typ.Name != DatetimeType && typ.Name != IntType {
				err = errors.Warning("sql: type of act column failed must be time.Time or int64").WithMeta("field", rt.Name)
				return
			}
			break
		case ambColumn:
			kind = Amb
			vw, typ.Name, err = NewBasicValueWriter(rt.Type)
			if err != nil {
				err = errors.Warning("sql: type of amb column failed must be int64 or string").WithCause(err).WithMeta("field", rt.Name)
				return
			}
			if typ.Name != StringType && typ.Name != IntType {
				err = errors.Warning("sql: type of amb column failed must be int64 or string").WithMeta("field", rt.Name)
				return
			}
			break
		case amtColumn:
			kind = Amt
			vw, typ.Name, err = NewBasicValueWriter(rt.Type)
			if err != nil {
				err = errors.Warning("sql: type of amt column failed must be time.Time or int64").WithCause(err).WithMeta("field", rt.Name)
				return
			}
			if typ.Name != DatetimeType && typ.Name != IntType {
				err = errors.Warning("sql: type of amt column failed must be time.Time or int64").WithMeta("field", rt.Name)
				return
			}
			break
		case adbColumn:
			kind = Adb
			vw, typ.Name, err = NewBasicValueWriter(rt.Type)
			if err != nil {
				err = errors.Warning("sql: type of adb column failed must be int64 or string").WithCause(err).WithMeta("field", rt.Name)
				return
			}
			if typ.Name != StringType && typ.Name != IntType {
				err = errors.Warning("sql: type of adb column failed must be int64 or string").WithMeta("field", rt.Name)
				return
			}
			break
		case adtColumn:
			kind = Adt
			vw, typ.Name, err = NewBasicValueWriter(rt.Type)
			if err != nil {
				err = errors.Warning("sql: type of adt column failed must be time.Time or int64").WithCause(err).WithMeta("field", rt.Name)
				return
			}
			if typ.Name != DatetimeType && typ.Name != IntType {
				err = errors.Warning("sql: type of adt column failed must be time.Time or int64").WithMeta("field", rt.Name)
				return
			}
			break
		case aolColumn:
			kind = Aol
			if rt.Type.Kind() == reflect.Int64 {
				typ.Name = IntType
				vw = &IntValue{}
			} else {
				err = errors.Warning("sql: type of aol column failed must be int64").WithMeta("field", rt.Name)
				return
			}
			break
		case incrColumn:
			if rt.Type.Kind() == reflect.Int64 {
				typ.Name = IntType
				vw = &IntValue{}
			} else {
				err = errors.Warning("sql: type of incr column failed must be int64").WithMeta("field", rt.Name)
				return
			}
			typ.Options = append(typ.Options, incrColumn)
		case jsonColumn:
			kind = Json
			typ.Name = JsonType
			vw = &JsonValue{
				ValueType: rt.Type,
			}
			break
		case virtualColumn:
			if len(items) < 3 {
				err = errors.Warning("sql: scan virtual column failed, kind and query are required").WithMeta("field", rt.Name)
				return
			}
			kind = Virtual
			vck := strings.ToLower(strings.TrimSpace(items[1]))
			valid := vck == "basic" || vck == "object" || vck == "array" || vck == "agg" || vck == "aggregate"
			if !valid {
				err = errors.Warning("sql: scan virtual column failed, kind is invalid").WithMeta("field", rt.Name)
				return
			}
			typ.Options = append(typ.Options, vck, strings.TrimSpace(items[2]))
			if vck == "object" || vck == "array" {
				typ.Name = JsonType
				vw = &JsonValue{
					ValueType: rt.Type,
				}
			} else {
				vw, typ.Name, err = NewBasicValueWriter(rt.Type)
				if err != nil {
					err = errors.Warning("sql: scan virtual column failed, kind is invalid").WithCause(err).WithMeta("field", rt.Name)
					return
				}
			}
			break
		case referenceColumn:
			if len(items) < 2 {
				err = errors.Warning("sql: scan reference column failed, mapping is required").WithMeta("field", rt.Name)
				return
			}
			kind = Reference
			typ.Options = append(typ.Options, strings.TrimSpace(items[1]))
			typ.Name = MappingType
			vw = &MappingValue{
				ValueType: rt.Type,
			}
			switch rt.Type.Kind() {
			case reflect.Struct:
				typ.Mapping, err = GetSpecification(ctx, reflect.Zero(rt.Type).Interface())
				if err != nil {
					err = errors.Warning("sql: scan reference column failed").WithMeta("field", rt.Name).WithCause(fmt.Errorf("reference column type must be implement Table")).WithCause(err)
					return
				}
				break
			case reflect.Ptr:
				typ.Mapping, err = GetSpecification(ctx, reflect.Zero(rt.Type.Elem()).Interface())
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
			vw = &MappingValue{
				ValueType: rt.Type,
			}
			switch rt.Type.Kind() {
			case reflect.Struct:
				typ.Mapping, err = GetSpecification(ctx, reflect.Zero(rt.Type).Interface())
				if err != nil {
					err = errors.Warning("sql: scan link column failed").WithMeta("field", rt.Name).WithCause(fmt.Errorf("link column type must be implement Table")).WithCause(err)
					return
				}
				break
			case reflect.Ptr:
				typ.Mapping, err = GetSpecification(ctx, reflect.Zero(rt.Type.Elem()).Interface())
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
			vw = &MappingValue{
				ValueType: rt.Type,
			}
			if rt.Type.Kind() != reflect.Slice {
				err = errors.Warning("sql: scan links column failed").WithMeta("field", rt.Name).WithCause(fmt.Errorf("links column type must be slice struct or slice ptr struct"))
				return
			}
			switch rt.Type.Elem().Kind() {
			case reflect.Struct:
				typ.Mapping, err = GetSpecification(ctx, reflect.Zero(rt.Type.Elem()).Interface())
				if err != nil {
					err = errors.Warning("sql: scan links column failed").WithMeta("field", rt.Name).WithCause(fmt.Errorf("links column type must be implement Table")).WithCause(err)
					return
				}
				break
			case reflect.Ptr:
				typ.Mapping, err = GetSpecification(ctx, reflect.Zero(rt.Type.Elem().Elem()).Interface())
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
			err = errors.Warning("sql: unknown column options").WithMeta("field", rt.Name).WithMeta("tag", tag)
			return
		}
	}
	if typ.Name == UnknownType {
		if rt.Type.ConvertibleTo(scannerType) && rt.Type.ConvertibleTo(jsonMarshalerType) {
			vw = &ScanValue{}
			typ.Name = ScanType
		} else {
			vw, typ.Name, err = NewBasicValueWriter(rt.Type)
			if err != nil {
				err = errors.Warning("sql: invalid column").WithCause(err).WithMeta("field", rt.Name).WithMeta("tag", tag)
				return
			}
		}
	}

	// json
	jsonTag, hasJsonTag := rt.Tag.Lookup(jsonColumn)
	if hasJsonTag {
		if idx := strings.IndexByte(jsonTag, ','); idx > 0 {
			jsonTag = jsonTag[0:idx]
		}
		jsonTag = strings.TrimSpace(jsonTag)
	} else {
		jsonTag = rt.Name
	}

	column = &Column{
		FieldIdx:    idx,
		Field:       rt.Name,
		Name:        name,
		JsonIdent:   jsonTag,
		Kind:        kind,
		Type:        typ,
		ValueWriter: vw,
	}

	if !column.Valid() {
		err = errors.Warning("sql: new column failed").WithMeta("field", rt.Name).WithCause(fmt.Errorf("%v is not supported", typ.Value))
		return
	}
	return
}
