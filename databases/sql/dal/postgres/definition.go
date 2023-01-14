package postgres

import "github.com/aacfactory/fns-contrib/databases/sql/dal"

type ColumnDefinition struct {
	name string
}

type ModelDefinition struct {
	schema                    string
	name                      string
	columns                   []*ColumnDefinition
	insertQuery               *GenericQuery
	insertOrUpdateQuery       *GenericQuery
	insertWhenExistOrNotQuery *GenericQuery
	updateQuery               *GenericQuery
	deleteQuery               *GenericQuery
}

type GenericQuery struct {
	method dal.QueryMethod
	query  string
}
