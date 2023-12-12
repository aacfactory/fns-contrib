package cmds

import (
	"github.com/redis/rueidis"
	"reflect"
	"strconv"
	"strings"
)

const (
	HDEL         = "HDEL"
	HEXISTS      = "HEXISTS"
	HGET         = "HGET"
	HGETALL      = "HGETALL"
	HINCRBY      = "HINCRBY"
	HINCRBYFLOAT = "HINCRBYFLOAT"
	HKEYS        = "HKEYS"
	HLEN         = "HLEN"
	HMGET        = "HMGET"
	HRANDFIELD   = "HRANDFIELD"
	HSCAN        = "HSCAN"
	HSET         = "HSET"
	HSETNX       = "HSETNX"
	HSTRLEN      = "HSTRLEN"
	HVALS        = "HVALS"
)

func registerHash() {
	builders[HDEL] = &HDELBuilder{}
	builders[HEXISTS] = &HEXISTSBuilder{}
	builders[HGET] = &HGETBuilder{}
	builders[HGETALL] = &HGETALLBuilder{}
	builders[HINCRBY] = &HINCRBYBuilder{}
	builders[HINCRBYFLOAT] = &HINCRBYFLOATBuilder{}
	builders[HKEYS] = &HKEYSBuilder{}
	builders[HLEN] = &HLENBuilder{}
	builders[HMGET] = &HMGETBuilder{}
	builders[HRANDFIELD] = &HRANDFIELDBuilder{}
	builders[HSCAN] = &HSCANBuilder{}
	builders[HSET] = &HSETBuilder{}
	builders[HSETNX] = &HSETNXBuilder{}
	builders[HSTRLEN] = &HSTRLENBuilder{}
	builders[HVALS] = &HVALSBuilder{}
}

type HDELBuilder struct {
}

func (b *HDELBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Hdel().Key(params[0]).Field(params[1:]...).Build()
	ok = true
	return
}

func (b *HDELBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type HEXISTSBuilder struct {
}

func (b *HEXISTSBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Hexists().Key(params[0]).Field(params[1]).Build()
	ok = true
	return
}

func (b *HEXISTSBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Hexists().Key(params[0]).Field(params[1]).Cache()
	ok = true
	return
}

type HGETBuilder struct {
}

func (b *HGETBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Hget().Key(params[0]).Field(params[1]).Build()
	ok = true
	return
}

func (b *HGETBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Hget().Key(params[0]).Field(params[1]).Cache()
	ok = true
	return
}

type HGETALLBuilder struct {
}

func (b *HGETALLBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Hgetall().Key(params[0]).Build()
	ok = true
	return
}

func (b *HGETALLBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Hgetall().Key(params[0]).Cache()
	ok = true
	return
}

type HINCRBYBuilder struct {
}

func (b *HINCRBYBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	delta, deltaErr := strconv.ParseInt(params[2], 10, 64)
	if deltaErr != nil {
		return
	}
	v = client.B().Hincrby().Key(params[0]).Field(params[1]).Increment(delta).Build()
	ok = true
	return
}

func (b *HINCRBYBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type HINCRBYFLOATBuilder struct {
}

func (b *HINCRBYFLOATBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	delta, deltaErr := strconv.ParseFloat(params[2], 64)
	if deltaErr != nil {
		return
	}
	v = client.B().Hincrbyfloat().Key(params[0]).Field(params[1]).Increment(delta).Build()
	ok = true
	return
}

func (b *HINCRBYFLOATBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type HKEYSBuilder struct {
}

func (b *HKEYSBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Hkeys().Key(params[0]).Build()
	ok = true
	return
}

func (b *HKEYSBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Hkeys().Key(params[0]).Cache()
	ok = true
	return
}

type HLENBuilder struct {
}

func (b *HLENBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Hlen().Key(params[0]).Build()
	ok = true
	return
}

func (b *HLENBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Hlen().Key(params[0]).Cache()
	ok = true
	return
}

type HMGETBuilder struct {
}

func (b *HMGETBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Hmget().Key(params[0]).Field(params[1:]...).Build()
	ok = true
	return
}

func (b *HMGETBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Hmget().Key(params[0]).Field(params[1:]...).Cache()
	ok = true
	return
}

type HRANDFIELDBuilder struct {
}

func (b *HRANDFIELDBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	if len(params) == 1 {
		v = client.B().Hrandfield().Key(params[0]).Build()
		ok = true
		return
	}
	rv := reflect.ValueOf(client.B().Hrandfield().Key(params[0]))
	params = params[1:]
	for _, param := range params {
		p, has := strings.CutPrefix(param, "COUNT:")
		if has {
			vv, vvErr := strconv.ParseInt(p, 10, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Count").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			break
		}
	}
	for _, param := range params {
		if param == "WITHVALUES" {
			rv = rv.MethodByName("Withvalues").Call([]reflect.Value{})[0]
			break
		}
	}

	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)

	ok = true
	return
}

func (b *HRANDFIELDBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type HSCANBuilder struct {
}

func (b *HSCANBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	cursor, cursorErr := strconv.ParseUint(params[1], 10, 64)
	if cursorErr != nil {
		return
	}
	if len(params) == 2 {
		v = client.B().Hscan().Key(params[0]).Cursor(cursor).Build()
		ok = true
		return
	}

	rv := reflect.ValueOf(client.B().Hscan().Key(params[0]).Cursor(cursor))

	for _, param := range params {
		match, has := strings.CutPrefix(param, "MATCH:")
		if has {
			rv = rv.MethodByName("Match").Call([]reflect.Value{reflect.ValueOf(match)})[0]
			break
		}
	}

	for _, param := range params {
		cv, has := strings.CutPrefix(param, "COUNT:")
		if has {
			count, countErr := strconv.ParseInt(cv, 10, 64)
			if countErr != nil {
				return
			}
			rv = rv.MethodByName("Count").Call([]reflect.Value{reflect.ValueOf(count)})[0]
			break
		}
	}

	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)

	ok = true
	return
}

func (b *HSCANBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type HSETBuilder struct {
}

func (b *HSETBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	rv := reflect.ValueOf(client.B().Hset().Key(params[0]).FieldValue())
	params = params[1:]
	for i := 0; i < len(params); i++ {
		field := params[i]
		value := params[i+1]
		i++
		rv = rv.MethodByName("FieldValue").Call([]reflect.Value{reflect.ValueOf(field), reflect.ValueOf(value)})[0]
	}
	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)
	ok = true
	return
}

func (b *HSETBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type HSETNXBuilder struct {
}

func (b *HSETNXBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Hsetnx().Key(params[0]).Field(params[1]).Value(params[2]).Build()
	ok = true
	return
}

func (b *HSETNXBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type HSTRLENBuilder struct {
}

func (b *HSTRLENBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Hstrlen().Key(params[0]).Field(params[1]).Build()
	ok = true
	return
}

func (b *HSTRLENBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Hstrlen().Key(params[0]).Field(params[1]).Cache()
	ok = true
	return
}

type HVALSBuilder struct {
}

func (b *HVALSBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Hvals().Key(params[0]).Build()
	ok = true
	return
}

func (b *HVALSBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Hvals().Key(params[0]).Cache()
	ok = true
	return
}
