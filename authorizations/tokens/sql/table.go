package sql

import "time"

var (
	_schema = ""
	_table  = ""
)

type TokenRow struct {
	Id       string    `col:"ID,pk" json:"ID,pk"`
	UserId   string    `col:"USER_ID" json:"USER_ID"`
	ExpireAT time.Time `col:"EXPIRE_AT" json:"EXPIRE_AT"`
	Token    string    `col:"TOKEN" json:"TOKEN"`
}

func (t *TokenRow) TableName() (schema string, table string) {
	schema = _schema
	table = _table
	return
}
