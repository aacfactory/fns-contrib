package sql

import (
	"fmt"
	"math"
	"strings"
	"time"
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
	p.sorts = append(p.sorts, &QuerySort{
		asc:    false,
		column: mapRelationName(column),
	})
	return p
}

func (p *QueryParam) ASC(column string) *QueryParam {
	p.sorts = append(p.sorts, &QuerySort{
		asc:    true,
		column: mapRelationName(column),
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

	p.pageNo = int(math.Ceil(float64(p.offset) / float64(p.length)))
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

func (p *QueryParam) mapToConditionString(alias string) (v string) {
	if len(p.conditions.values) > 0 {
		v = p.conditions.toString(alias)
	}
	return
}

func (p *QueryParam) mapToSortsString(alias string) (v string) {
	if len(p.sorts) > 0 {
		alias = mapRelationName(alias)
		v = " ORDER BY"
		for i, sort := range p.sorts {
			if i == 0 {
				if sort.asc {
					v = v + " " + alias + "." + mapRelationName(sort.column)
				} else {
					v = v + " " + alias + "." + mapRelationName(sort.column) + " DESC"
				}
			} else {
				if sort.asc {
					v = v + "," + alias + "." + mapRelationName(sort.column)
				} else {
					v = v + "," + alias + "." + mapRelationName(sort.column) + " DESC"
				}
			}
		}
	}
	return
}

func (p *QueryParam) mapToRangeString(alias string) (v string) {
	if p.length > 0 {
		alias = mapRelationName(alias)
		v = v + " OFFSET " + fmt.Sprintf("%d", p.offset) + " LIMIT " + fmt.Sprintf("%d", p.length)
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

func (c *QueryConditions) toString(alias string) string {
	alias = mapRelationName(alias)
	conditions := " WHERE 1=1"
	for _, value := range c.values {
		op := value.op
		switch op {
		case "eq":
			x := value.values[0]
			switch x.(type) {
			case string:
				v := x.(string)
				if strings.Index(v, "(") == 0 && strings.LastIndex(v, ")") == len(v)-1 {
					conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " = " + v
				} else {
					conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " = " + fmt.Sprintf("'%s'", v)
				}
			case int, int8, int32, int64, uint, uint8, uint16, uint32, uint64:
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " = " + fmt.Sprintf("%d", x)
			case float32, float64:
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " = " + fmt.Sprintf("%f", x)
			case time.Time:
				xx := x.(time.Time).Format("2006-01-02 15:04:05")
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " = " + fmt.Sprintf("'%s'", xx)
			}
		case "not":
			x := value.values[0]
			switch x.(type) {
			case string:
				v := x.(string)
				if strings.Index(v, "(") == 0 && strings.LastIndex(v, ")") == len(v)-1 {
					conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " <> " + v
				} else {
					conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " <> " + fmt.Sprintf("'%s'", v)
				}
			case int, int8, int32, int64, uint, uint8, uint16, uint32, uint64:
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " <> " + fmt.Sprintf("%d", x)
			case float32, float64:
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " <> " + fmt.Sprintf("%f", x)
			case time.Time:
				xx := x.(time.Time).Format("2006-01-02 15:04:05")
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " <> " + fmt.Sprintf("'%s'", xx)
			}
		case "gt":
			x := value.values[0]
			switch x.(type) {
			case string:
				v := x.(string)
				if strings.Index(v, "(") == 0 && strings.LastIndex(v, ")") == len(v)-1 {
					conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " > " + v
				} else {
					conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " > " + fmt.Sprintf("'%s'", v)
				}
			case int, int8, int32, int64, uint, uint8, uint16, uint32, uint64:
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " > " + fmt.Sprintf("%d", x)
			case float32, float64:
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " > " + fmt.Sprintf("%f", x)
			case time.Time:
				xx := x.(time.Time).Format("2006-01-02 15:04:05")
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " > " + fmt.Sprintf("'%s'", xx)
			}
		case "lt":
			x := value.values[0]
			switch x.(type) {
			case string:
				v := x.(string)
				if strings.Index(v, "(") == 0 && strings.LastIndex(v, ")") == len(v)-1 {
					conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " < " + v
				} else {
					conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " < " + fmt.Sprintf("'%s'", v)
				}
			case int, int8, int32, int64, uint, uint8, uint16, uint32, uint64:
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " < " + fmt.Sprintf("%d", x)
			case float32, float64:
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " < " + fmt.Sprintf("%f", x)
			case time.Time:
				xx := x.(time.Time).Format("2006-01-02 15:04:05")
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " < " + fmt.Sprintf("'%s'", xx)
			}
		case "gte":
			x := value.values[0]
			switch x.(type) {
			case string:
				v := x.(string)
				if strings.Index(v, "(") == 0 && strings.LastIndex(v, ")") == len(v)-1 {
					conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " >= " + v
				} else {
					conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " >= " + fmt.Sprintf("'%s'", v)
				}
			case int, int8, int32, int64, uint, uint8, uint16, uint32, uint64:
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " >= " + fmt.Sprintf("%d", x)
			case float32, float64:
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " >= " + fmt.Sprintf("%f", x)
			case time.Time:
				xx := x.(time.Time).Format("2006-01-02 15:04:05")
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " >= " + fmt.Sprintf("'%s'", xx)
			}
		case "lte":
			x := value.values[0]
			switch x.(type) {
			case string:
				v := x.(string)
				if strings.Index(v, "(") == 0 && strings.LastIndex(v, ")") == len(v)-1 {
					conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " <= " + v
				} else {
					conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " <= " + fmt.Sprintf("'%s'", v)
				}
			case int, int8, int32, int64, uint, uint8, uint16, uint32, uint64:
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " <= " + fmt.Sprintf("%d", x)
			case float32, float64:
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " <= " + fmt.Sprintf("%f", x)
			case time.Time:
				xx := x.(time.Time).Format("2006-01-02 15:04:05")
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " <= " + fmt.Sprintf("'%s'", xx)
			}
		case "like":
			x := value.values[0]
			switch x.(type) {
			case string:
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " LIKE '" + x.(string) + "%'"
			}
		case "between":
			beg := value.values[0]
			end := value.values[1]
			switch beg.(type) {
			case int, int8, int32, int64, uint, uint8, uint16, uint32, uint64:
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " BETWEEN " + fmt.Sprintf("%d", beg) + " AND " + fmt.Sprintf("%d", end)
			case float32, float64:
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " BETWEEN " + fmt.Sprintf("%f", beg) + " AND " + fmt.Sprintf("%f", end)
			case time.Time:
				x := beg.(time.Time).Format("2006-01-02 15:04:05")
				y := end.(time.Time).Format("2006-01-02 15:04:05")
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " BETWEEN " + fmt.Sprintf("'%s'", x) + " AND " + fmt.Sprintf("'%s'", y)
			}
		case "in":
			switch value.values[0].(type) {
			case string:
				block := ""
				if len(value.values) == 1 {
					v := value.values[0].(string)
					if strings.Index(v, "(") == 0 && strings.LastIndex(v, ")") == len(v)-1 {
						block = v
					}
					conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " IN " + block
				} else {
					for i, x := range value.values {
						if i == 0 {
							block = fmt.Sprintf("'%s'", x)
						} else {
							block = block + ", " + fmt.Sprintf("'%s'", x)
						}
					}
					conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " IN (" + block + ")"
				}
			case int, int8, int32, int64, uint, uint8, uint16, uint32, uint64:
				block := ""
				for i, x := range value.values {
					if i == 0 {
						block = fmt.Sprintf("%d", x)
					} else {
						block = block + ", " + fmt.Sprintf("%d", x)
					}
				}
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " IN (" + block + ")"
			case float32, float64:
				block := ""
				for i, x := range value.values {
					if i == 0 {
						block = fmt.Sprintf("%f", x)
					} else {
						block = block + ", " + fmt.Sprintf("%f", x)
					}
				}
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " IN (" + block + ")"
			case time.Time:
				block := ""
				for i, x := range value.values {
					xx := x.(time.Time).Format("2006-01-02 15:04:05")
					if i == 0 {
						block = fmt.Sprintf("'%s'", xx)
					} else {
						block = block + ", " + fmt.Sprintf("'%s'", xx)
					}
				}
				conditions = conditions + " AND " + alias + "." + mapRelationName(value.column) + " IN (" + block + ")"
			}
		}
	}
	return conditions
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
