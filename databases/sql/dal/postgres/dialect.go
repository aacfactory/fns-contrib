package postgres

import "github.com/aacfactory/fns-contrib/databases/sql/dal"

var dialect = dal.Dialect("postgres")

func Dialect() dal.Option {
	return dal.WithDialect(dialect)
}

func newQueryGenerator() (generator *QueryGenerator) {

	return
}

type QueryGenerator struct {
}

func (generator *QueryGenerator) InsertOrUpdate(structure *dal.ModelStructure, model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) InsertWhenExist(structure *dal.ModelStructure, model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) InsertWhenNotExist(structure *dal.ModelStructure, model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) Update(structure *dal.ModelStructure, model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) Delete(structure *dal.ModelStructure, model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) Exist(structure *dal.ModelStructure, cond *dal.Conditions) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) Count(structure *dal.ModelStructure, cond *dal.Conditions) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) Get(structure *dal.ModelStructure, cond *dal.Conditions) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) Query(structure *dal.ModelStructure, cond *dal.Conditions) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) QueryWithRange(structure *dal.ModelStructure, cond *dal.Conditions, orders *dal.Orders, rng *dal.Range) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) Page(structure *dal.ModelStructure, cond *dal.Conditions, orders *dal.Orders, rng *dal.Range) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}
