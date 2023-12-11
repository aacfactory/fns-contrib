package cmds

import "github.com/redis/rueidis"

// string
const (
	APPEND      = "APPEND"
	DECR        = "DECR"
	DECRBY      = "DECRBY"
	GET         = "GET"
	GETDEL      = "GETDEL"
	GETEX       = "GETEX"
	GETRANGE    = "GETRANGE"
	GETSET      = "GETSET"
	INCR        = "INCR"
	INCRBY      = "INCRBY"
	INCRBYFLOAT = "INCRBYFLOAT"
	LCS         = "LCS"
	MGET        = "MGET"
	MSET        = "MSET"
	MSETNX      = "MSETNX"
	SET         = "SET"
	SETRANGE    = "SETRANGE"
	STRLEN      = "STRLEN"
)

func registerString() {
	builders[APPEND] = &AppendBuilder{}

}

type AppendBuilder struct {
}

func (b *AppendBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Append().Key(params[0]).Value(params[1]).Build()
	ok = true
	return
}

func (b *AppendBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}
