package sql

import (
	"database/sql"
	stdJson "encoding/json"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/databases"
	"github.com/aacfactory/fns/commons/times"
	"github.com/aacfactory/json"
	"reflect"
	"strings"
	"time"
)

type Row []Column

func NewRows(rows databases.Rows) (v Rows, err error) {
	names, namesErr := rows.Columns()
	if namesErr != nil {
		_ = rows.Close()
		err = errors.Warning("sql: new rows failed").WithCause(namesErr)
		return
	}
	cts, ctsErr := rows.ColumnTypes()
	if ctsErr != nil {
		_ = rows.Close()
		err = errors.Warning("sql: new rows failed").WithCause(ctsErr)
		return
	}
	columnLen := len(cts)
	columnTypes := make([]ColumnType, 0, columnLen)
	for i, ct := range cts {
		columnTypes = append(columnTypes, NewColumnType(names[i], strings.ToUpper(ct.DatabaseType), ct.ScanType))
	}
	v = Rows{
		idx:         0,
		rows:        rows,
		columnTypes: columnTypes,
		columnLen:   columnLen,
		values:      nil,
		size:        0,
	}
	return
}

type Rows struct {
	idx         int
	rows        databases.Rows
	columnTypes []ColumnType
	columnLen   int
	values      []Row
	size        int
}

func (rows *Rows) Columns() []ColumnType {
	return rows.columnTypes
}

func (rows *Rows) Close() error {
	if rows.rows == nil {
		return nil
	}
	return rows.rows.Close()
}

func (rows Rows) MarshalJSON() (p []byte, err error) {
	if len(rows.values) > 0 {
		tr := transferRows{
			ColumnTypes: rows.columnTypes,
			Values:      rows.values,
		}
		p, err = json.Marshal(tr)
		return
	}
	if rows.idx != 0 {
		err = errors.Warning("sql: encode rows failed").WithCause(fmt.Errorf("rows has been used"))
		return
	}
	mc := newMultiColumns(rows.columnLen)
	for rows.rows.Next() {
		scanners := mc.Next()
		scanErr := rows.rows.Scan(scanners...)
		if scanErr != nil {
			_ = rows.rows.Close()
			mc.Release()
			err = errors.Warning("sql: encode rows failed").WithCause(scanErr)
			return
		}
	}
	_ = rows.rows.Close()
	rows.values = mc.Rows()
	rows.size = len(rows.values)
	tr := transferRows{
		ColumnTypes: rows.columnTypes,
		Values:      rows.values,
	}
	p, err = json.Marshal(tr)
	mc.Release()
	return
}

func (rows *Rows) UnmarshalJSON(p []byte) (err error) {
	tr := transferRows{}
	err = json.Unmarshal(p, &tr)
	if err != nil {
		return
	}
	rows.idx = 0
	rows.columnTypes = tr.ColumnTypes
	rows.columnLen = len(rows.columnTypes)
	rows.values = tr.Values
	rows.size = len(rows.values)
	return
}

func (rows *Rows) Next() (ok bool) {
	if rows.rows != nil {
		ok = rows.rows.Next()
		return
	}
	ok = rows.idx < rows.size
	if ok {
		rows.idx++
	}
	return
}

// Scan
// element of dst must be scanned.
// in dac case, when field is json kind and type does not implement sql.NullJson,
// then wrap field value by sql.NullJson
func (rows *Rows) Scan(dst ...any) (err error) {
	if rows.rows != nil {
		err = rows.rows.Scan(dst...)
		return
	}
	if rows.idx > rows.size {
		err = sql.ErrNoRows
		return
	}
	dstLen := len(dst)
	if dstLen == 0 {
		return
	}
	if dstLen != rows.columnLen {
		err = errors.Warning("sql: scan failed").WithCause(fmt.Errorf("size is not matched"))
		return
	}
	row := rows.values[rows.idx-1]
	for i := 0; i < rows.columnLen; i++ {
		item := dst[i]
		if item == nil {
			err = errors.Warning("sql: scan failed").WithCause(fmt.Errorf("some of dst is nil"))
			return
		}
		column := row[i]
		if !column.Valid {
			continue
		}
		ct := rows.columnTypes[i]
		switch d := item.(type) {
		case *string:
			cv, cvErr := column.String()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			*d = cv
			break
		case *NullString:
			cv, cvErr := column.String()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			d.Valid = len(cv) > 0
			d.String = cv
			break
		case *sql.NullString:
			cv, cvErr := column.String()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			d.Valid = len(cv) > 0
			d.String = cv
			break
		case *bool:
			cv, cvErr := column.Bool()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			*d = cv
			break
		case *NullBool:
			cv, cvErr := column.Bool()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			d.Valid = true
			d.Bool = cv
			break
		case *sql.NullBool:
			cv, cvErr := column.Bool()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			d.Valid = true
			d.Bool = cv
			break
		case *int64:
			cv, cvErr := column.Int()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			*d = cv
			break
		case *int32:
			cv, cvErr := column.Int()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			*d = int32(cv)
			break
		case *int16:
			cv, cvErr := column.Int()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			*d = int16(cv)
			break
		case NullInt64:
			cv, cvErr := column.Int()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			d.Valid = true
			d.Int64 = cv
			break
		case *sql.NullInt64:
			cv, cvErr := column.Int()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			d.Valid = true
			d.Int64 = cv
			break
		case NullInt32:
			cv, cvErr := column.Int()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			d.Valid = true
			d.Int32 = int32(cv)
			break
		case *sql.NullInt32:
			cv, cvErr := column.Int()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			d.Valid = true
			d.Int32 = int32(cv)
			break
		case NullInt16:
			cv, cvErr := column.Int()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			d.Valid = true
			d.Int16 = int16(cv)
			break
		case *sql.NullInt16:
			cv, cvErr := column.Int()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			d.Valid = true
			d.Int16 = int16(cv)
			break
		case *float64:
			cv, cvErr := column.Float()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			*d = cv
			break
		case *float32:
			cv, cvErr := column.Float()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			*d = float32(cv)
			break
		case sql.NullFloat64:
			cv, cvErr := column.Float()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			d.Valid = true
			d.Float64 = cv
			break
		case *sql.NullFloat64:
			cv, cvErr := column.Float()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			d.Valid = true
			d.Float64 = cv
			break
		case *time.Time:
			cv, cvErr := column.Datetime()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			*d = cv
			break
		case *NullDatetime:
			cv, cvErr := column.Datetime()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			d.Valid = true
			d.Time = cv
			break
		case *sql.NullTime:
			cv, cvErr := column.Datetime()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			d.Valid = !cv.IsZero()
			d.Time = cv
			break
		case *times.Date:
			cv, cvErr := column.Date()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			*d = cv
			break
		case *NullDate:
			cv, cvErr := column.Date()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			d.Valid = !cv.IsZero()
			d.Date = cv
			break
		case *times.Time:
			cv, cvErr := column.Time()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			*d = cv
			break
		case *NullTime:
			cv, cvErr := column.Time()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			d.Valid = !cv.IsZero()
			d.Time = cv
			break
		case *[]byte:
			cv, cvErr := column.Bytes()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			*d = cv
			break
		case *NullBytes:
			cv, cvErr := column.Bytes()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			d.Valid = true
			d.Bytes = cv
			break
		case *json.RawMessage:
			cv, cvErr := column.Bytes()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			*d = cv
			break
		case *stdJson.RawMessage:
			cv, cvErr := column.Bytes()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			*d = cv
			break
		case *sql.RawBytes:
			cv, cvErr := column.Bytes()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			*d = cv
			break
		case *byte:
			cv, cvErr := column.Byte()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			*d = cv
			break
		case *sql.NullByte:
			cv, cvErr := column.Byte()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			d.Valid = true
			d.Byte = cv
			break
		case *json.Date:
			cv, cvErr := column.Date()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			*d = json.NewDate(cv.Year, cv.Month, cv.Day)
			break
		case *json.Time:
			cv, cvErr := column.Time()
			if cvErr != nil {
				err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
				return
			}
			*d = json.NewTime(cv.Hour, cv.Minutes, cv.Second)
			break
		default:
			scanner, isScanner := item.(sql.Scanner)
			if isScanner {
				var scanErr error
				switch ct.Type {
				case "string":
					cv, cvErr := column.String()
					if cvErr != nil {
						err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
						return
					}
					scanErr = scanner.Scan(cv)
					break
				case "bool":
					cv, cvErr := column.Bool()
					if cvErr != nil {
						err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
						return
					}
					scanErr = scanner.Scan(cv)
					break
				case "int":
					cv, cvErr := column.Int()
					if cvErr != nil {
						err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
						return
					}
					scanErr = scanner.Scan(cv)
					break
				case "float":
					cv, cvErr := column.Float()
					if cvErr != nil {
						err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
						return
					}
					scanErr = scanner.Scan(cv)
					break
				case "datetime":
					cv, cvErr := column.Datetime()
					if cvErr != nil {
						err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
						return
					}
					scanErr = scanner.Scan(cv)
					break
				case "date":
					cv, cvErr := column.Datetime()
					if cvErr != nil {
						err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
						return
					}
					scanErr = scanner.Scan(cv)
					break
				case "time":
					cv, cvErr := column.Datetime()
					if cvErr != nil {
						err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
						return
					}
					scanErr = scanner.Scan(cv)
					break
				case "bytes":
					cv, cvErr := column.Bytes()
					if cvErr != nil {
						err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
						return
					}
					scanErr = scanner.Scan(cv)
					break
				case "byte":
					cv, cvErr := column.Byte()
					if cvErr != nil {
						err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
						return
					}
					scanErr = scanner.Scan(cv)
					break
				default:
					err = errors.Warning("sql: scan failed").WithCause(fmt.Errorf("type is unsupported")).WithMeta("column", ct.Name)
					return
				}
				if scanErr != nil {
					err = errors.Warning("sql: scan failed").WithCause(scanErr).WithMeta("column", ct.Name)
					return
				}
				return
			}
			if ct.Type == "json" {
				decodeErr := json.Unmarshal(column.Value, item)
				if decodeErr != nil {
					err = errors.Warning("sql: scan failed").WithCause(decodeErr).WithMeta("column", ct.Name)
					return
				}
				return
			}
			rv := reflect.ValueOf(item).Elem()
			switch rv.Type().Kind() {
			case reflect.String:
				cv, cvErr := column.String()
				if cvErr != nil {
					err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
					return
				}
				rv.SetString(cv)
				break
			case reflect.Bool:
				cv, cvErr := column.Bool()
				if cvErr != nil {
					err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
					return
				}
				rv.SetBool(cv)
				break
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				cv, cvErr := column.Int()
				if cvErr != nil {
					err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
					return
				}
				rv.SetInt(cv)
				break
			case reflect.Float32, reflect.Float64:
				cv, cvErr := column.Float()
				if cvErr != nil {
					err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
					return
				}
				rv.SetFloat(cv)
				break
			case reflect.Uint8:
				cv, cvErr := column.Byte()
				if cvErr != nil {
					err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
					return
				}
				rv.SetUint(uint64(cv))
				break
			default:
				rt := rv.Type()
				if rt.ConvertibleTo(datetimeType) {
					cv, cvErr := column.Datetime()
					if cvErr != nil {
						err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
						return
					}
					rv.Set(reflect.ValueOf(cv).Convert(rt))
				} else if rt.ConvertibleTo(dateType) {
					cv, cvErr := column.Date()
					if cvErr != nil {
						err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
						return
					}
					rv.Set(reflect.ValueOf(cv).Convert(rt))
				} else if rt.ConvertibleTo(timeType) {
					cv, cvErr := column.Time()
					if cvErr != nil {
						err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
						return
					}
					rv.Set(reflect.ValueOf(cv).Convert(rt))
				} else if rt.ConvertibleTo(rawType) {
					cv, cvErr := column.Bytes()
					if cvErr != nil {
						err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
						return
					}
					rv.Set(reflect.ValueOf(cv).Convert(rt))
				} else if rt.ConvertibleTo(bytesType) {
					cv, cvErr := column.Bytes()
					if cvErr != nil {
						err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
						return
					}
					rv.Set(reflect.ValueOf(cv).Convert(rt))
				} else if rt.ConvertibleTo(jsonDateType) {
					cv, cvErr := column.Date()
					if cvErr != nil {
						err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
						return
					}
					rv.Set(reflect.ValueOf(cv).Convert(rt))
				} else if rt.ConvertibleTo(jsonTimeType) {
					cv, cvErr := column.Time()
					if cvErr != nil {
						err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
						return
					}
					rv.Set(reflect.ValueOf(cv).Convert(rt))
				} else {
					unmarshaler, isUnmarshaler := item.(json.Unmarshaler)
					if isUnmarshaler {
						cv, cvErr := column.Bytes()
						if cvErr != nil {
							err = errors.Warning("sql: scan failed").WithCause(cvErr).WithMeta("column", ct.Name)
							return
						}
						decodeErr := unmarshaler.UnmarshalJSON(cv)
						if decodeErr != nil {
							err = errors.Warning("sql: scan failed").WithCause(decodeErr).WithMeta("column", ct.Name)
							return
						}
						return
					}
					cv, cvErr := column.Bytes()
					if cvErr != nil {
						err = errors.Warning("sql: scan failed").WithCause(fmt.Errorf("unsupported type")).WithMeta("column", ct.Name)
						return
					}
					decodeErr := json.Unmarshal(cv, item)
					if decodeErr != nil {
						err = errors.Warning("sql: scan failed").WithCause(decodeErr).WithMeta("column", ct.Name)
						return
					}
					return
				}
			}
			break
		}
	}
	return
}

type transferRows struct {
	ColumnTypes []ColumnType `json:"columnTypes"`
	Values      []Row        `json:"values"`
}
