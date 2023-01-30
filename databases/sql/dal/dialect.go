package dal

import "fmt"

type Dialect string

func RegisterDialectQueryGeneratorBuilder(dialect Dialect, builder DialectQueryGeneratorBuilder) {
	dialectQueryGeneratorBuilders[dialect] = builder
}

func getDialectQueryGeneratorBuilder(dialect Dialect) (builder DialectQueryGeneratorBuilder, has bool) {
	if dialect == "" {
		builder, has = getDefaultDialectQueryGenerator()
		return
	}
	builder, has = dialectQueryGeneratorBuilders[dialect]
	return
}

func getDefaultDialectQueryGenerator() (builder DialectQueryGeneratorBuilder, has bool) {
	if defaultQueryGeneratorBuilder != nil {
		builder = defaultQueryGeneratorBuilder
		has = true
		return
	}
	v, err, _ := gettingBarrier.Do(defaultQueryGeneratorGettingKey, func() (gen interface{}, err error) {
		if len(dialectQueryGeneratorBuilders) != 1 {
			err = fmt.Errorf("empty or too many query generator builders")
			return
		}
		for _, queryGeneratorBuilder := range dialectQueryGeneratorBuilders {
			defaultQueryGeneratorBuilder = queryGeneratorBuilder
			gen = defaultQueryGeneratorBuilder
			return
		}
		return
	})
	if err != nil {
		return
	}
	builder = v.(DialectQueryGeneratorBuilder)
	return
}

var (
	dialectQueryGeneratorBuilders                              = make(map[Dialect]DialectQueryGeneratorBuilder)
	defaultQueryGeneratorBuilder  DialectQueryGeneratorBuilder = nil
)

const (
	defaultQueryGeneratorGettingKey = "@default_query_generator@"
)

const (
	Query   = QueryMethod("query")
	Execute = QueryMethod("execute")
)

type QueryMethod string

type QueryGenerator interface {
	Insert(model Model) (method QueryMethod, query string, arguments []interface{}, err error)
	InsertOrUpdate(model Model) (method QueryMethod, query string, arguments []interface{}, err error)
	InsertWhenExist(model Model) (method QueryMethod, query string, arguments []interface{}, err error)
	InsertWhenNotExist(model Model) (method QueryMethod, query string, arguments []interface{}, err error)
	Update(model Model) (method QueryMethod, query string, arguments []interface{}, err error)
	Delete(model Model) (method QueryMethod, query string, arguments []interface{}, err error)
	Exist(cond *Conditions) (method QueryMethod, query string, arguments []interface{}, err error)
	Count(cond *Conditions) (method QueryMethod, query string, arguments []interface{}, err error)
	Get(cond *Conditions) (method QueryMethod, query string, arguments []interface{}, err error)
	Query(cond *Conditions) (method QueryMethod, query string, arguments []interface{}, err error)
	QueryWithRange(cond *Conditions, orders *Orders, rng *Range) (method QueryMethod, query string, arguments []interface{}, err error)
	Page(cond *Conditions, orders *Orders, rng *Range) (method QueryMethod, query string, arguments []interface{}, err error)
}

type DialectQueryGeneratorBuilder interface {
	Build(structure *ModelStructure) (generator QueryGenerator, err error)
}
