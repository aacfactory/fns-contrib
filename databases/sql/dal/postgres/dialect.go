package postgres

import (
	"context"
	"github.com/aacfactory/fns-contrib/databases/sql/dal"
)

var dialect = dal.Dialect("postgres")

func Dialect() dal.Option {
	return dal.WithDialect(dialect)
}

type QueryGeneratorBuilder struct {
}

func (builder *QueryGeneratorBuilder) Build(structure *dal.ModelStructure) (generator dal.QueryGenerator, err error) {
	generator = &QueryGenerator{
		insertQuery:             newInsertGenericQuery(structure),
		insertOrUpdateQuery:     newInsertOrUpdateGenericQuery(structure),
		insertWhenExistQuery:    newInsertWhenExistGenericQuery(structure),
		insertWhenNotExistQuery: newInsertWhenNotExistGenericQuery(structure),
		updateQuery:             newUpdateGenericQuery(structure),
		deleteQuery:             newDeleteGenericQuery(structure),
		existQuery:              newExistGenericQuery(structure),
		countQuery:              newCountGenericQuery(structure),
		getQuery:                newSelectGenericQuery(structure),
		pageQuery:               newPageGenericQuery(structure),
	}
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
	pageQuery               *GenericQuery
}

func (generator *QueryGenerator) Insert(ctx context.Context, model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query, arguments, err = generator.insertQuery.WeaveExecute(ctx, model)
	return
}

func (generator *QueryGenerator) InsertOrUpdate(ctx context.Context, model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query, arguments, err = generator.insertOrUpdateQuery.WeaveExecute(ctx, model)
	return
}

func (generator *QueryGenerator) InsertWhenExist(ctx context.Context, model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query, arguments, err = generator.insertWhenExistQuery.WeaveExecute(ctx, model)
	return
}

func (generator *QueryGenerator) InsertWhenNotExist(ctx context.Context, model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query, arguments, err = generator.insertWhenNotExistQuery.WeaveExecute(ctx, model)
	return
}

func (generator *QueryGenerator) Update(ctx context.Context, model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query, arguments, err = generator.updateQuery.WeaveExecute(ctx, model)
	return
}

func (generator *QueryGenerator) Delete(ctx context.Context, model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query, arguments, err = generator.deleteQuery.WeaveExecute(ctx, model)
	return
}

func (generator *QueryGenerator) Exist(ctx context.Context, cond *dal.Conditions) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query, arguments, err = generator.existQuery.WeaveQuery(ctx, cond, nil, nil)
	return
}

func (generator *QueryGenerator) Count(ctx context.Context, cond *dal.Conditions) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query, arguments, err = generator.countQuery.WeaveQuery(ctx, cond, nil, nil)
	return
}

func (generator *QueryGenerator) Query(ctx context.Context, cond *dal.Conditions, orders *dal.Orders, rng *dal.Range) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query, arguments, err = generator.getQuery.WeaveQuery(ctx, cond, orders, rng)
	return
}

func (generator *QueryGenerator) Page(ctx context.Context, cond *dal.Conditions, orders *dal.Orders, rng *dal.Range) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query, arguments, err = generator.pageQuery.WeaveQuery(ctx, cond, orders, rng)
	return
}
