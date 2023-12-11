package cmds

import (
	"github.com/redis/rueidis"
	"reflect"
	"strconv"
)

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
	builders[DECR] = &DecrBuilder{}
	builders[DECRBY] = &DecrByBuilder{}
	builders[GET] = &GetBuilder{}
	builders[GETDEL] = &GetDelBuilder{}
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

type DecrBuilder struct {
}

func (b *DecrBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Decr().Key(params[0]).Build()
	ok = true
	return
}

func (b *DecrBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type DecrByBuilder struct {
}

func (b *DecrByBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	by, byErr := strconv.ParseInt(params[1], 10, 64)
	if byErr != nil {
		return
	}
	v = client.B().Decrby().Key(params[0]).Decrement(by).Build()
	ok = true
	return
}

func (b *DecrByBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type GetBuilder struct {
}

func (b *GetBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Get().Key(params[0]).Build()
	ok = true
	return
}

func (b *GetBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Get().Key(params[0]).Cache()
	ok = true
	return
}

type GetDelBuilder struct {
}

func (b *GetDelBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Getdel().Key(params[0]).Build()
	ok = true
	return
}

func (b *GetDelBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type GetExBuilder struct {
}

func (b *GetExBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	key := params[0]
	if len(params) == 1 {
		v = client.B().Getex().Key(key).Build()
		ok = true
		return
	}
	params = params[1:]
	rv := reflect.ValueOf(client.B().Getex().Key(key))
	// Persist
	// Ex duration
	// Px duration
	// Exat time
	// Pxat time

	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)
	ok = true
	return
}

func (b *GetExBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}
