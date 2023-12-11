package cmds

import (
	"github.com/redis/rueidis"
	"reflect"
	"strconv"
	"strings"
	"time"
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
	builders[GETEX] = &GetExBuilder{}
	builders[GETRANGE] = &GetRangeBuilder{}
	builders[GETSET] = &GetSetBuilder{}
	builders[INCR] = &IncrBuilder{}
	builders[INCRBY] = &IncrByBuilder{}
	builders[INCRBYFLOAT] = &IncrByFloatBuilder{}
	builders[LCS] = &LCSBuilder{}
	builders[MGET] = &MGetBuilder{}
	builders[MSET] = &MSetBuilder{}
	builders[MSETNX] = &MSetNxBuilder{}
	builders[SET] = &SetBuilder{}
	builders[SETRANGE] = &SetRangeBuilder{}
	builders[STRLEN] = &StrLenBuilder{}
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
	for _, param := range params {
		if param == "PERSIST" {
			rv = rv.MethodByName("Persist").Call([]reflect.Value{})[0]
			break
		}
	}
	// Ex duration
	for _, param := range params {
		ex, has := strings.CutPrefix(param, "EX:")
		if has {
			dur, durErr := time.ParseDuration(ex)
			if durErr != nil {
				return
			}
			rv = rv.MethodByName("Ex").Call([]reflect.Value{reflect.ValueOf(dur)})[0]
			break
		}
	}
	// Px duration
	for _, param := range params {
		ex, has := strings.CutPrefix(param, "PX:")
		if has {
			dur, durErr := time.ParseDuration(ex)
			if durErr != nil {
				return
			}
			rv = rv.MethodByName("Px").Call([]reflect.Value{reflect.ValueOf(dur)})[0]
			break
		}
	}
	// Exat time
	for _, param := range params {
		ex, has := strings.CutPrefix(param, "EXAT:")
		if has {
			dur, durErr := time.Parse(ex, time.RFC3339)
			if durErr != nil {
				return
			}
			rv = rv.MethodByName("Exat").Call([]reflect.Value{reflect.ValueOf(dur)})[0]
			break
		}
	}
	// Pxat time
	for _, param := range params {
		ex, has := strings.CutPrefix(param, "PXAT:")
		if has {
			dur, durErr := time.Parse(ex, time.RFC3339)
			if durErr != nil {
				return
			}
			rv = rv.MethodByName("Pxat").Call([]reflect.Value{reflect.ValueOf(dur)})[0]
			break
		}
	}
	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)
	ok = true
	return
}

func (b *GetExBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type GetRangeBuilder struct {
}

func (b *GetRangeBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	start, startErr := strconv.ParseInt(params[1], 10, 64)
	if startErr != nil {
		return
	}
	end, endErr := strconv.ParseInt(params[2], 10, 64)
	if endErr != nil {
		return
	}
	v = client.B().Getrange().Key(params[0]).Start(start).End(end).Build()
	ok = true
	return
}

func (b *GetRangeBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	start, startErr := strconv.ParseInt(params[1], 10, 64)
	if startErr != nil {
		return
	}
	end, endErr := strconv.ParseInt(params[2], 10, 64)
	if endErr != nil {
		return
	}
	v = client.B().Getrange().Key(params[0]).Start(start).End(end).Cache()
	ok = true
	return
}

type GetSetBuilder struct {
}

func (b *GetSetBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Getset().Key(params[0]).Value(params[1]).Build()
	ok = true
	return
}

func (b *GetSetBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type IncrBuilder struct {
}

func (b *IncrBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Incr().Key(params[0]).Build()
	ok = true
	return
}

func (b *IncrBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type IncrByBuilder struct {
}

func (b *IncrByBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	by, byErr := strconv.ParseInt(params[1], 10, 64)
	if byErr != nil {
		return
	}
	v = client.B().Incrby().Key(params[0]).Increment(by).Build()
	ok = true
	return
}

func (b *IncrByBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type IncrByFloatBuilder struct {
}

func (b *IncrByFloatBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	by, byErr := strconv.ParseFloat(params[1], 64)
	if byErr != nil {
		return
	}
	v = client.B().Incrbyfloat().Key(params[0]).Increment(by).Build()
	ok = true
	return
}

func (b *IncrByFloatBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type LCSBuilder struct {
}

func (b *LCSBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	key1 := params[0]
	key2 := params[1]
	if len(params) == 2 {
		v = client.B().Lcs().Key1(key1).Key2(key2).Build()
		ok = true
		return
	}
	rv := reflect.ValueOf(client.B().Lcs().Key1(key1).Key2(key2))
	params = params[2:]
	// LEN
	for _, param := range params {
		if param == "LEN" {
			rv = rv.MethodByName("Len").Call([]reflect.Value{})[0]
			break
		}
	}
	// IDX
	for _, param := range params {
		if param == "IDX" {
			rv = rv.MethodByName("Idx").Call([]reflect.Value{})[0]
			break
		}
	}
	// MINMATCHLEN int64
	for _, param := range params {
		v, has := strings.CutPrefix(param, "MINMATCHLEN:")
		if has {
			n, nErr := strconv.ParseInt(v, 10, 64)
			if nErr != nil {
				return
			}
			rv = rv.MethodByName("Minmatchlen").Call([]reflect.Value{reflect.ValueOf(n)})[0]
			break
		}
	}
	// WITHMATCHLEN
	for _, param := range params {
		if param == "WITHMATCHLEN" {
			rv = rv.MethodByName("Withmatchlen").Call([]reflect.Value{})[0]
			break
		}
	}
	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)

	ok = true
	return
}

func (b *LCSBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type MGetBuilder struct {
}

func (b *MGetBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Mget().Key(params...).Build()
	ok = true
	return
}

func (b *MGetBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Mget().Key(params...).Cache()
	ok = true
	return
}

type MSetBuilder struct {
}

func (b *MSetBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	rv := reflect.ValueOf(client.B().Mset().KeyValue())
	for i := 0; i < len(params); i++ {
		key := params[i]
		value := params[i+1]
		rv = rv.MethodByName("KeyValue").Call([]reflect.Value{reflect.ValueOf(key), reflect.ValueOf(value)})[0]
		i++
	}
	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)
	ok = true
	return
}

func (b *MSetBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type MSetNxBuilder struct {
}

func (b *MSetNxBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	rv := reflect.ValueOf(client.B().Msetnx().KeyValue())
	for i := 0; i < len(params); i++ {
		key := params[i]
		value := params[i+1]
		rv = rv.MethodByName("KeyValue").Call([]reflect.Value{reflect.ValueOf(key), reflect.ValueOf(value)})[0]
		i++
	}
	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)
	ok = true
	return
}

func (b *MSetNxBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type SetBuilder struct {
}

func (b *SetBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	if len(params) == 2 {
		v = client.B().Set().Key(params[0]).Value(params[1]).Build()
		ok = true
		return
	}
	rv := reflect.ValueOf(client.B().Set().Key(params[0]).Value(params[1]))
	params = params[2:]
	// NX
	for _, param := range params {
		if param == "NX" {
			rv = rv.MethodByName("Nx").Call([]reflect.Value{})[0]
			break
		}
	}
	// XX
	for _, param := range params {
		if param == "XX" {
			rv = rv.MethodByName("Xx").Call([]reflect.Value{})[0]
			break
		}
	}
	// GET
	for _, param := range params {
		if param == "GET" {
			rv = rv.MethodByName("Get").Call([]reflect.Value{})[0]
			break
		}
	}
	// EX
	for _, param := range params {
		p, has := strings.CutPrefix(param, "EX:")
		if has {
			v, vErr := time.ParseDuration(p)
			if vErr != nil {
				return
			}
			rv = rv.MethodByName("Ex").Call([]reflect.Value{reflect.ValueOf(v)})[0]
			break
		}
	}
	// PX
	for _, param := range params {
		p, has := strings.CutPrefix(param, "PX:")
		if has {
			v, vErr := time.ParseDuration(p)
			if vErr != nil {
				return
			}
			rv = rv.MethodByName("Px").Call([]reflect.Value{reflect.ValueOf(v)})[0]
			break
		}
	}
	// EXAT
	for _, param := range params {
		p, has := strings.CutPrefix(param, "EXAT:")
		if has {
			v, vErr := time.Parse(p, time.RFC3339)
			if vErr != nil {
				return
			}
			rv = rv.MethodByName("Exat").Call([]reflect.Value{reflect.ValueOf(v)})[0]
			break
		}
	}
	// PXAT
	for _, param := range params {
		p, has := strings.CutPrefix(param, "PXAT:")
		if has {
			v, vErr := time.Parse(p, time.RFC3339)
			if vErr != nil {
				return
			}
			rv = rv.MethodByName("Pxat").Call([]reflect.Value{reflect.ValueOf(v)})[0]
			break
		}
	}
	// KEEPTTL
	for _, param := range params {
		if param == "KEEPTTL" {
			rv = rv.MethodByName("Keepttl").Call([]reflect.Value{})[0]
			break
		}
	}

	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)

	ok = true
	return
}

func (b *SetBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type SetRangeBuilder struct {
}

func (b *SetRangeBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	offset, offsetErr := strconv.ParseInt(params[1], 10, 64)
	if offsetErr != nil {
		return
	}
	v = client.B().Setrange().Key(params[0]).Offset(offset).Value(params[2]).Build()
	ok = true
	return
}

func (b *SetRangeBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type StrLenBuilder struct {
}

func (b *StrLenBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Strlen().Key(params[0]).Build()
	ok = true
	return
}

func (b *StrLenBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Strlen().Key(params[0]).Cache()
	ok = true
	return
}
