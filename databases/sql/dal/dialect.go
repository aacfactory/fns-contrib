package dal

import "fmt"

type Dialect string

func RegisterDialectQueryGenerator(dialect Dialect, generator QueryGenerator) {
	dialectQueryGenerators[dialect] = generator
}

func getDialectQueryGenerator(dialect Dialect) (generator QueryGenerator, has bool) {
	generator, has = dialectQueryGenerators[dialect]
	return
}

func getDefaultDialectQueryGenerator() (generator QueryGenerator, has bool) {
	if defaultQueryGenerator != nil {
		generator = defaultQueryGenerator
		has = true
		return
	}
	v, err, _ := gettingBarrier.Do(defaultQueryGeneratorGettingKey, func() (gen interface{}, err error) {
		if len(dialectQueryGenerators) != 1 {
			err = fmt.Errorf("empty or too many query generators")
			return
		}
		for _, queryGenerator := range dialectQueryGenerators {
			defaultQueryGenerator = queryGenerator
			gen = defaultQueryGenerator
			return
		}
		return
	})
	if err != nil {
		return
	}
	generator = v.(QueryGenerator)
	return
}

var (
	dialectQueryGenerators                = make(map[Dialect]QueryGenerator)
	defaultQueryGenerator  QueryGenerator = nil
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
	Insert(structure *ModelStructure, model Model) (method QueryMethod, query string, arguments []interface{}, err error)
	InsertOrUpdate(structure *ModelStructure, model Model) (method QueryMethod, query string, arguments []interface{}, err error)
	InsertWhenExist(structure *ModelStructure, model Model) (method QueryMethod, query string, arguments []interface{}, err error)
	InsertWhenNotExist(structure *ModelStructure, model Model) (method QueryMethod, query string, arguments []interface{}, err error)
	Update(structure *ModelStructure, model Model) (method QueryMethod, query string, arguments []interface{}, err error)
	Delete(structure *ModelStructure, model Model) (method QueryMethod, query string, arguments []interface{}, err error)
	Exist(structure *ModelStructure, cond *Conditions) (method QueryMethod, query string, arguments []interface{}, err error)
	Count(structure *ModelStructure, cond *Conditions) (method QueryMethod, query string, arguments []interface{}, err error)
	Get(structure *ModelStructure, cond *Conditions) (method QueryMethod, query string, arguments []interface{}, err error)
	Query(structure *ModelStructure, cond *Conditions) (method QueryMethod, query string, arguments []interface{}, err error)
	QueryWithRange(structure *ModelStructure, cond *Conditions, orders *Orders, rng *Range) (method QueryMethod, query string, arguments []interface{}, err error)
	Page(structure *ModelStructure, cond *Conditions, orders *Orders, rng *Range) (method QueryMethod, query string, arguments []interface{}, err error)
}
