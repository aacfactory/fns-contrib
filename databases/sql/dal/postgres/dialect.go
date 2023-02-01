package postgres

import "github.com/aacfactory/fns-contrib/databases/sql/dal"

var dialect = dal.Dialect("postgres")

func Dialect() dal.Option {
	return dal.WithDialect(dialect)
}

type QueryGeneratorBuilder struct {
}

func (builder *QueryGeneratorBuilder) Build(structure *dal.ModelStructure) (generator dal.QueryGenerator, err error) {

	return
}

type QueryGenerator struct {
	insertQuery             *GenericQuery
	insertOrUpdateQuery     *GenericQuery
	insertWhenExistQuery    *GenericQuery
	insertWhenNotExistQuery *GenericQuery
	updateQuery             *GenericQuery
	deleteQuery             *GenericQuery
	existQuery              *GenericQuery
	countQuery              *GenericQuery
	getQuery                *GenericQuery
	query                   *GenericQuery
	pageQuery               *GenericQuery
}

func (generator *QueryGenerator) Insert(model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) InsertOrUpdate(model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) InsertWhenExist(model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) InsertWhenNotExist(model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) Update(model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) Delete(model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) Exist(cond *dal.Conditions) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) Count(cond *dal.Conditions) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) Get(cond *dal.Conditions) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) Query(cond *dal.Conditions) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) QueryWithRange(cond *dal.Conditions, orders *dal.Orders, rng *dal.Range) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}

func (generator *QueryGenerator) Page(cond *dal.Conditions, orders *dal.Orders, rng *dal.Range) (method dal.QueryMethod, query string, arguments []interface{}, err error) {

	return
}
