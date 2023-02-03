package dal

import (
	"context"
	"fmt"
)

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
	Insert(ctx context.Context, model Model) (method QueryMethod, query string, arguments []interface{}, err error)
	InsertOrUpdate(ctx context.Context, model Model) (method QueryMethod, query string, arguments []interface{}, err error)
	InsertWhenExist(ctx context.Context, model Model) (method QueryMethod, query string, arguments []interface{}, err error)
	InsertWhenNotExist(ctx context.Context, model Model) (method QueryMethod, query string, arguments []interface{}, err error)
	Update(ctx context.Context, model Model) (method QueryMethod, query string, arguments []interface{}, err error)
	Delete(ctx context.Context, model Model) (method QueryMethod, query string, arguments []interface{}, err error)
	Exist(ctx context.Context, cond *Conditions) (method QueryMethod, query string, arguments []interface{}, err error)
	Count(ctx context.Context, cond *Conditions) (method QueryMethod, query string, arguments []interface{}, err error)
	Query(ctx context.Context, cond *Conditions, orders *Orders, rng *Range) (method QueryMethod, query string, arguments []interface{}, err error)
	Page(ctx context.Context, cond *Conditions, orders *Orders, rng *Range) (method QueryMethod, query string, arguments []interface{}, err error)
}

type DialectQueryGeneratorBuilder interface {
	Build(structure *ModelStructure) (generator QueryGenerator, err error)
}
