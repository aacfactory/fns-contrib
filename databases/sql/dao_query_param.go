package sql

import (
	"fmt"
	"math"
	"strings"
)

func NewQueryParam() *QueryParam {
	return &QueryParam{
		conditions: NewQueryConditions(),
		sorts:      make([]*QuerySort, 0, 1),
		offset:     0,
		length:     0,
	}
}

type QueryParam struct {
	conditions *QueryConditions
	sorts      []*QuerySort
	offset     int
	length     int
	pageNo     int
	pageSize   int
}

func (p *QueryParam) Conditions() *QueryConditions {
	return p.conditions
}

func (p *QueryParam) DESC(column string) *QueryParam {
	if dialect == "postgres" {
		column = tableInfoConvertToPostgresName(column)
	}
	p.sorts = append(p.sorts, &QuerySort{
		asc:    false,
		column: column,
	})
	return p
}

func (p *QueryParam) ASC(column string) *QueryParam {
	if dialect == "postgres" {
		column = tableInfoConvertToPostgresName(column)
	}
	p.sorts = append(p.sorts, &QuerySort{
		asc:    true,
		column: column,
	})
	return p
}

func (p *QueryParam) Range(offset int, length int) *QueryParam {
	if offset < 0 {
		offset = 0
	}
	if length < 1 {
		length = math.MaxInt
	}
	p.offset = offset
	p.length = length
	if p.offset == 0 {
		p.pageNo = 1
	} else {
		p.pageNo = int(math.Ceil(float64(p.offset) / float64(p.length)))
	}
	p.pageSize = p.length
	return p
}

func (p *QueryParam) Page(no int, size int) *QueryParam {
	if no < 1 {
		no = 1
	}
	if size < 1 {
		size = 10
	}
	p.pageNo = no
	p.pageSize = size
	p.offset = (p.pageNo - 1) * p.pageSize
	p.length = p.pageSize
	return p
}

func (p *QueryParam) mapToConditionString(alias string, args *Tuple) (v string) {
	if len(p.conditions.values) > 0 {
		v = p.conditions.map0(alias, args)
	}
	return
}

func (p *QueryParam) mapToSortsString(alias string) (v string) {
	if len(p.sorts) > 0 {
		if dialect == "postgres" {
			alias = tableInfoConvertToPostgresName(alias)
		}
		v = " ORDER BY"
		for i, sort := range p.sorts {
			column := sort.column
			if dialect == "postgres" {
				column = tableInfoConvertToPostgresName(column)
			}
			if i == 0 {
				if sort.asc {
					v = v + " " + alias + "." + column
				} else {
					v = v + " " + alias + "." + column + " DESC"
				}
			} else {
				if sort.asc {
					v = v + "," + alias + "." + column
				} else {
					v = v + "," + alias + "." + column + " DESC"
				}
			}
		}
	}
	return
}

func (p *QueryParam) mapToRangeString() (v string) {
	if p.length > 0 {
		switch dialect {
		case "postgres":
			v = v + " OFFSET " + fmt.Sprintf("%d", p.offset) + " LIMIT " + fmt.Sprintf("%d", p.length)
		case "mysql":
			v = v + " LIMIT " + fmt.Sprintf("%d", p.offset) + "," + fmt.Sprintf("%d", p.length)
		default:
			panic(fmt.Sprintf("fns SQL: use DAO failed for %s dialect is not supported", dialect))
		}
	}
	return
}

type QuerySort struct {
	asc    bool
	column string
}

type QueryCondition struct {
	column string
	op     string
	values []interface{}
}

func NewQueryConditions() *QueryConditions {
	return &QueryConditions{
		values: make([]*QueryCondition, 0, 1),
	}
}

type QueryConditions struct {
	values []*QueryCondition
}

func (c *QueryConditions) map0(alias string, args *Tuple) (conditions string) {
	argIdx := 0
	for _, value := range c.values {
		argIdx++
		op := value.op
		column := value.column
		mark := "?"
		if dialect == "postgres" {
			column = tableInfoConvertToPostgresName(column)
			mark = fmt.Sprintf("$%d", argIdx)
		}
		switch op {
		case "eq":
			x := value.values[0]
			switch x.(type) {
			case string:
				v := x.(string)
				if strings.Index(v, "(") == 0 && strings.LastIndex(v, ")") == len(v)-1 && strings.Contains(strings.ToUpper(v), "SELECT") {
					conditions = conditions + " AND " + alias + "." + column + " = " + v
				} else {
					conditions = conditions + " AND " + alias + "." + column + " = " + mark
					args.Append(x)
				}
			default:
				conditions = conditions + " AND " + alias + "." + column + " = " + mark
				args.Append(x)
			}
		case "not":
			x := value.values[0]
			switch x.(type) {
			case string:
				v := x.(string)
				if strings.Index(v, "(") == 0 && strings.LastIndex(v, ")") == len(v)-1 && strings.Contains(strings.ToUpper(v), "SELECT") {
					conditions = conditions + " AND " + alias + "." + column + " <> " + v
				} else {
					conditions = conditions + " AND " + alias + "." + column + " <> " + mark
					args.Append(x)
				}
			default:
				conditions = conditions + " AND " + alias + "." + column + " <> " + mark
				args.Append(x)
			}
		case "gt":
			x := value.values[0]
			switch x.(type) {
			case string:
				v := x.(string)
				if strings.Index(v, "(") == 0 && strings.LastIndex(v, ")") == len(v)-1 && strings.Contains(strings.ToUpper(v), "SELECT") {
					conditions = conditions + " AND " + alias + "." + column + " > " + v
				} else {
					conditions = conditions + " AND " + alias + "." + column + " > " + mark
					args.Append(x)
				}
			default:
				conditions = conditions + " AND " + alias + "." + column + " > " + mark
				args.Append(x)
			}
		case "lt":
			x := value.values[0]
			switch x.(type) {
			case string:
				v := x.(string)
				if strings.Index(v, "(") == 0 && strings.LastIndex(v, ")") == len(v)-1 && strings.Contains(strings.ToUpper(v), "SELECT") {
					conditions = conditions + " AND " + alias + "." + column + " < " + v
				} else {
					conditions = conditions + " AND " + alias + "." + column + " < " + mark
					args.Append(x)
				}
			default:
				conditions = conditions + " AND " + alias + "." + column + " < " + mark
				args.Append(x)
			}
		case "gte":
			x := value.values[0]
			switch x.(type) {
			case string:
				v := x.(string)
				if strings.Index(v, "(") == 0 && strings.LastIndex(v, ")") == len(v)-1 && strings.Contains(strings.ToUpper(v), "SELECT") {
					conditions = conditions + " AND " + alias + "." + column + " >= " + v
				} else {
					conditions = conditions + " AND " + alias + "." + column + " >= " + mark
					args.Append(x)
				}
			default:
				conditions = conditions + " AND " + alias + "." + column + " >= " + mark
				args.Append(x)
			}
		case "lte":
			x := value.values[0]
			switch x.(type) {
			case string:
				v := x.(string)
				if strings.Index(v, "(") == 0 && strings.LastIndex(v, ")") == len(v)-1 && strings.Contains(strings.ToUpper(v), "SELECT") {
					conditions = conditions + " AND " + alias + "." + column + " <= " + v
				} else {
					conditions = conditions + " AND " + alias + "." + column + " <= " + mark
					args.Append(x)
				}
			default:
				conditions = conditions + " AND " + alias + "." + column + " <= " + mark
				args.Append(x)
			}
		case "like":
			x := value.values[0]
			switch x.(type) {
			case string:
				conditions = conditions + " AND " + alias + "." + column + " LIKE '" + x.(string) + "%'"
			}
		case "between":
			beg := value.values[0]
			end := value.values[1]
			argIdx++
			endMark := "?"
			if dialect == "postgres" {
				endMark = fmt.Sprintf("$%d", argIdx)
			}
			conditions = conditions + " AND " + alias + "." + column + " BETWEEN " + mark + " AND " + endMark
			args.Append(beg, end)
		case "in":
			switch value.values[0].(type) {
			case string:
				block := ""
				if len(value.values) == 1 {
					v := value.values[0].(string)
					if strings.Index(v, "(") == 0 && strings.LastIndex(v, ")") == len(v)-1 && strings.Contains(strings.ToUpper(v), "SELECT") {
						block = v
					}
					conditions = conditions + " AND " + alias + "." + column + " IN " + block
				} else {
					for i, x := range value.values {
						inMark := "?"
						if dialect == "postgres" {
							inMark = fmt.Sprintf("$%d", argIdx)
						}
						if i == 0 {
							block = inMark
						} else {
							argIdx++
							block = block + ", " + inMark
						}
						args.Append(x)
					}
					conditions = conditions + " AND " + alias + "." + column + " IN (" + block + ")"
				}
			default:
				block := ""
				for i, x := range value.values {
					inMark := "?"
					if dialect == "postgres" {
						inMark = fmt.Sprintf("$%d", argIdx)
					}
					if i == 0 {
						block = inMark
					} else {
						argIdx++
						block = block + ", " + inMark
					}
					args.Append(x)
				}
				conditions = conditions + " AND " + alias + "." + column + " IN (" + block + ")"
			}
		}
	}
	conditions = "WHERE " + conditions[5:]
	return
}

func (c *QueryConditions) Eq(column string, value interface{}) *QueryConditions {
	if value == nil {
		return c
	}
	c.values = append(c.values, &QueryCondition{
		column: column,
		op:     "eq",
		values: []interface{}{value},
	})
	return c
}

func (c *QueryConditions) NotEq(column string, value interface{}) *QueryConditions {
	if value == nil {
		return c
	}
	c.values = append(c.values, &QueryCondition{
		column: column,
		op:     "not",
		values: []interface{}{value},
	})
	return c
}

func (c *QueryConditions) GT(column string, value interface{}) *QueryConditions {
	if value == nil {
		return c
	}
	c.values = append(c.values, &QueryCondition{
		column: column,
		op:     "gt",
		values: []interface{}{value},
	})
	return c
}

func (c *QueryConditions) LT(column string, value interface{}) *QueryConditions {
	if value == nil {
		return c
	}
	c.values = append(c.values, &QueryCondition{
		column: column,
		op:     "lt",
		values: []interface{}{value},
	})
	return c
}

func (c *QueryConditions) GTE(column string, value interface{}) *QueryConditions {
	if value == nil {
		return c
	}
	c.values = append(c.values, &QueryCondition{
		column: column,
		op:     "gte",
		values: []interface{}{value},
	})
	return c
}

func (c *QueryConditions) LTE(column string, value interface{}) *QueryConditions {
	if value == nil {
		return c
	}
	c.values = append(c.values, &QueryCondition{
		column: column,
		op:     "lte",
		values: []interface{}{value},
	})
	return c
}

func (c *QueryConditions) Like(column string, value interface{}) *QueryConditions {
	if value == nil {
		return c
	}
	c.values = append(c.values, &QueryCondition{
		column: column,
		op:     "like",
		values: []interface{}{value},
	})
	return c
}

func (c *QueryConditions) Between(column string, beg interface{}, end interface{}) *QueryConditions {
	if beg == nil || end == nil {
		return c
	}
	c.values = append(c.values, &QueryCondition{
		column: column,
		op:     "between",
		values: []interface{}{beg, end},
	})
	return c
}

func (c *QueryConditions) In(column string, values ...interface{}) *QueryConditions {
	if values == nil || len(values) == 0 {
		return c
	}
	c.values = append(c.values, &QueryCondition{
		column: column,
		op:     "in",
		values: values,
	})
	return c
}
