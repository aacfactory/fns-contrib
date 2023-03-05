package sql

import (
	"github.com/aacfactory/fns-contrib/databases/sql/internal"
	"github.com/aacfactory/json"
)

type transactionRegistration struct {
	Id string `json:"id"`
}

type transactionStatus struct {
	Finished bool `json:"finished"`
}

type dialectArgument struct {
	Database string `json:"database"`
}

type dialectResult struct {
	Dialect string `json:"dialect"`
}

type transactionBeginArgument struct {
	Database string `json:"database"`
}

type transactionCommitArgument struct {
	Database string `json:"database"`
}

type transactionRollbackArgument struct {
	Database string `json:"database"`
}

type queryArgument struct {
	Database string          `json:"database"`
	Query    string          `json:"query"`
	Args     *internal.Tuple `json:"args"`
}

type executeArgument struct {
	Database string          `json:"database"`
	Query    string          `json:"query"`
	Args     *internal.Tuple `json:"args"`
}

type executeResult struct {
	Affected     int64 `json:"affected"`
	LastInsertId int64 `json:"lastInsertId"`
}

type Column interface {
	Type() (typ string)
	Name() (v string)
	IsNil() (ok bool)
	Get(v interface{}) (err error)
	RawValue() (raw []byte)
}

type Row interface {
	json.Marshaler
	json.Unmarshaler
	Empty() (ok bool)
	Columns() (columns []Column)
	Column(name string, value interface{}) (has bool, err error)
}

type Rows interface {
	json.Marshaler
	json.Unmarshaler
	Empty() (ok bool)
	Size() int
	Next() (v Row, has bool)
}
