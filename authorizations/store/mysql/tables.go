package mysql

import "time"

type TokenRow struct {
	Id        string    `col:"ID,pk" json:"ID,pk"`
	UserId    string    `col:"USER_ID" json:"USER_ID"`
	NotBefore time.Time `col:"NOT_BEFORE" json:"NOT_BEFORE"`
	NotAfter  time.Time `col:"NOT_AFTER" json:"NOT_AFTER"`
	Value     string    `col:"VALUE" json:"VALUE"`
	schema    string
	name      string
}

func (t TokenRow) TableName() (schema string, table string) {
	schema = t.schema
	table = t.name
	return
}
