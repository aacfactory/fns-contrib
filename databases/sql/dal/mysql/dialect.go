package mysql

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/sql/dal"
	"github.com/aacfactory/fns/context"
	"strings"
)

var dialect = dal.Dialect("mysql")

type QueryGeneratorBuilder struct {
}

func (builder *QueryGeneratorBuilder) Build(structure *dal.ModelStructure) (generator dal.QueryGenerator, err error) {
	generator = &QueryGenerator{
		insertQuery:             newInsertQuery(structure),
		insertOrUpdateQuery:     newInsertOrUpdateQuery(structure),
		insertWhenExistQuery:    newInsertWhenExistQuery(structure),
		insertWhenNotExistQuery: newInsertWhenNotExistQuery(structure),
		updateQuery:             newUpdateQuery(structure),
		deleteQuery:             newDeleteQuery(structure),
		existQuery:              newExistQuery(structure),
		countQuery:              newCountQuery(structure),
		selectQuery:             newSelectQuery(structure),
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
	selectQuery             *GenericQuery
}

func (generator *QueryGenerator) Insert(ctx context.Context, model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query, arguments, err = generator.insertQuery.WeaveExecute(ctx, model)
	return
}

func (generator *QueryGenerator) InsertOrUpdate(ctx context.Context, model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	if generator.insertOrUpdateQuery == nil {
		err = fmt.Errorf("can not do insert or update cause there is no conflict column")
		return
	}
	method, query, arguments, err = generator.insertOrUpdateQuery.WeaveExecute(ctx, model)
	return
}

func (generator *QueryGenerator) InsertWhenExist(ctx context.Context, model dal.Model, source string) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query, arguments, err = generator.insertWhenExistQuery.WeaveExecute(ctx, model)
	query = strings.Replace(query, "$$SOURCE_QUERY$$", source, 1)
	return
}

func (generator *QueryGenerator) InsertWhenNotExist(ctx context.Context, model dal.Model, source string) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query, arguments, err = generator.insertWhenNotExistQuery.WeaveExecute(ctx, model)
	query = strings.Replace(query, "$$SOURCE_QUERY$$", source, 1)
	return
}

func (generator *QueryGenerator) Update(ctx context.Context, model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query, arguments, err = generator.updateQuery.WeaveExecute(ctx, model)
	return
}

func (generator *QueryGenerator) UpdateWithValues(ctx context.Context, values dal.Values, cond *dal.Conditions) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query, arguments, err = generator.updateQuery.WeaveUpdateWithValues(ctx, values, cond)
	return
}

func (generator *QueryGenerator) Delete(ctx context.Context, model dal.Model) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query, arguments, err = generator.deleteQuery.WeaveExecute(ctx, model)
	return
}

func (generator *QueryGenerator) DeleteWithConditions(ctx context.Context, cond *dal.Conditions) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query, arguments, err = generator.deleteQuery.WeaveDeleteWithConditions(ctx, cond)
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

func (generator *QueryGenerator) Select(ctx context.Context, cond *dal.Conditions, orders *dal.Orders, rng *dal.Range) (method dal.QueryMethod, query string, arguments []interface{}, err error) {
	method, query, arguments, err = generator.selectQuery.WeaveQuery(ctx, cond, orders, rng)
	return
}
