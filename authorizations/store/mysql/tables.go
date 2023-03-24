package mysql

import "time"

var (
	_schema = ""
	_table  = ""
)

type TokenRow struct {
	Id        string    `col:"ID,pk" json:"ID,pk"`
	UserId    string    `col:"USER_ID" json:"USER_ID"`
	NotBefore time.Time `col:"NOT_BEFORE" json:"NOT_BEFORE"`
	NotAfter  time.Time `col:"NOT_AFTER" json:"NOT_AFTER"`
	Value     string    `col:"VALUE" json:"VALUE"`
}

func (t *TokenRow) TableName() (schema string, table string) {
	schema = _schema
	table = _table
	return
}
