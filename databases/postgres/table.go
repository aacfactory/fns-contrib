package postgres

const (
	tagName = "col"
)

type Table interface {
	TableName() (schema string, table string)
}

type table struct {
	Schema  string
	Name    string
	Columns []*column
}

func (t *table) generateInsertSQL() (query string, columns []*column) {

	return
}

func (t *table) generateInsertWhenExistOrNotSQL(exist bool, sourceSQL string) (query string, columns []*column) {

	return
}

func (t *table) generateUpdateSQL() (query string, columns []*column) {

	return
}

func (t *table) generateDeleteSQL() (query string, columns []*column) {

	return
}

func (t *table) generateInsertOrUpdateSQL() (query string, columns []*column) {

	return
}

func (t *table) generateExistSQL(conditions *Conditions) (query string) {

	return
}

func (t *table) generateCountSQL(conditions *Conditions) (query string) {

	return
}

func (t *table) generateQuerySQL(conditions *Conditions, rng *Range, orders []*Order) (query string) {

	return
}
