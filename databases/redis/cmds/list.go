package cmds

import (
	"github.com/redis/rueidis"
	"reflect"
	"strconv"
	"strings"
)

const (
	BLMOVE  = "BLMOVE"
	BLMPOP  = "BLMPOP"
	BLPOP   = "BLPOP"
	BRPOP   = "BRPOP"
	LINDEX  = "LINDEX"
	LINSERT = "LINSERT"
	LLEN    = "LLEN"
	LMOVE   = "LMOVE"
	LMPOP   = "LMPOP"
	LPOP    = "LPOP"
	LPOS    = "LPOS"
	LPUSH   = "LPUSH"
	LPUSHX  = "LPUSHX"
	LRANGE  = "LRANGE"
	LREM    = "LREM"
	LSET    = "LSET"
	LTRIM   = "LTRIM"
	RPOP    = "RPOP"
	RPUSH   = "RPUSH"
	RPUSHX  = "RPUSHX"
)

func registerList() {
	builders[BLMOVE] = &BLMOVEBuilder{}
	builders[BLMPOP] = &BLMPOPBuilder{}
	builders[BLPOP] = &BLPOPBuilder{}
	builders[BRPOP] = &BRPOPBuilder{}
	builders[LINDEX] = &LINDEXBuilder{}
	builders[LINSERT] = &LINSERTBuilder{}
	builders[LLEN] = &LLENBuilder{}
	builders[LMOVE] = &LMOVEBuilder{}
	builders[LMPOP] = &LMPOPBuilder{}
	builders[LPOP] = &LPOPBuilder{}
	builders[LPOS] = &LPOSBuilder{}
	builders[LPUSH] = &LPUSHBuilder{}
	builders[LPUSHX] = &LPUSHXBuilder{}
	builders[LRANGE] = &LRANGEBuilder{}
	builders[LREM] = &LREMBuilder{}
	builders[LSET] = &LSETBuilder{}
	builders[LTRIM] = &LTRIMBuilder{}
	builders[RPOP] = &RPOPBuilder{}
	builders[RPUSH] = &RPUSHBuilder{}
	builders[RPUSHX] = &RPUSHXBuilder{}
}

type BLMOVEBuilder struct {
}

func (b *BLMOVEBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {

	rv := reflect.ValueOf(client.B().Blmove().Source(params[0]).Destination(params[1]))

	params = params[2:]

	for _, param := range params {
		if param == "LEFT" {
			rv = rv.MethodByName("Left").Call([]reflect.Value{})[0]
			continue
		}
		if param == "RIGHT" {
			rv = rv.MethodByName("Right").Call([]reflect.Value{})[0]
			continue
		}
		if timeout, has := strings.CutPrefix(param, "TIMEOUT:"); has {
			vv, vvErr := strconv.ParseFloat(timeout, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Timeout").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			continue
		}
	}

	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)

	ok = true
	return
}

func (b *BLMOVEBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type BLMPOPBuilder struct {
}

func (b *BLMPOPBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {

	timeout, timeoutErr := strconv.ParseFloat(params[0], 64)
	if timeoutErr != nil {
		return
	}
	nk, nkErr := strconv.ParseInt(params[1], 10, 64)
	if nkErr != nil {
		return
	}

	rv := reflect.ValueOf(client.B().Blmpop().Timeout(timeout).Numkeys(nk))

	params = params[2:]

	for _, param := range params {
		if key, has := strings.CutPrefix(param, "KEY:"); has {
			rv = rv.MethodByName("Key").Call([]reflect.Value{reflect.ValueOf(key)})[0]
			continue
		}
		if param == "LEFT" {
			rv = rv.MethodByName("Left").Call([]reflect.Value{})[0]
			continue
		}
		if param == "RIGHT" {
			rv = rv.MethodByName("Right").Call([]reflect.Value{})[0]
			continue
		}
		if count, has := strings.CutPrefix(param, "COUNT:"); has {
			vv, vvErr := strconv.ParseInt(count, 10, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Count").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			continue
		}
	}

	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)

	ok = true
	return
}

func (b *BLMPOPBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type BLPOPBuilder struct {
}

func (b *BLPOPBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {

	rv := reflect.ValueOf(client.B().Blpop())

	for _, param := range params {
		if key, has := strings.CutPrefix(param, "KEY:"); has {
			rv = rv.MethodByName("Key").Call([]reflect.Value{reflect.ValueOf(key)})[0]
			continue
		}
		if timeout, has := strings.CutPrefix(param, "TIMEOUT:"); has {
			vv, vvErr := strconv.ParseFloat(timeout, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Timeout").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			continue
		}
	}

	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)

	ok = true
	return
}

func (b *BLPOPBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type BRPOPBuilder struct {
}

func (b *BRPOPBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {

	rv := reflect.ValueOf(client.B().Brpop())

	for _, param := range params {
		if key, has := strings.CutPrefix(param, "KEY:"); has {
			rv = rv.MethodByName("Key").Call([]reflect.Value{reflect.ValueOf(key)})[0]
			continue
		}
		if timeout, has := strings.CutPrefix(param, "TIMEOUT:"); has {
			vv, vvErr := strconv.ParseFloat(timeout, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Timeout").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			continue
		}
	}

	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)

	ok = true
	return
}

func (b *BRPOPBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type LINDEXBuilder struct {
}

func (b *LINDEXBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	index, indexErr := strconv.ParseInt(params[1], 10, 64)
	if indexErr != nil {
		return
	}
	v = client.B().Lindex().Key(params[0]).Index(index).Build()
	ok = true
	return
}

func (b *LINDEXBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	index, indexErr := strconv.ParseInt(params[1], 10, 64)
	if indexErr != nil {
		return
	}
	v = client.B().Lindex().Key(params[0]).Index(index).Cache()
	ok = true
	return
}

type LINSERTBuilder struct {
}

func (b *LINSERTBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	if params[1] == "AFTER" {
		v = client.B().Linsert().Key(params[0]).After().Pivot(params[2]).Element(params[3]).Build()
	} else {
		v = client.B().Linsert().Key(params[0]).Before().Pivot(params[2]).Element(params[3]).Build()
	}
	ok = true
	return
}

func (b *LINSERTBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type LLENBuilder struct {
}

func (b *LLENBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Llen().Key(params[0]).Build()
	ok = true
	return
}

func (b *LLENBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Llen().Key(params[0]).Cache()
	ok = true
	return
}

type LMOVEBuilder struct {
}

func (b *LMOVEBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	rv := reflect.ValueOf(client.B().Lmove().Source(params[0]).Destination(params[1]))

	params = params[2:]

	for _, param := range params {
		if param == "LEFT" {
			rv = rv.MethodByName("Left").Call([]reflect.Value{})[0]
			continue
		}
		if param == "RIGHT" {
			rv = rv.MethodByName("Right").Call([]reflect.Value{})[0]
			continue
		}
	}

	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)

	ok = true
	return
}

func (b *LMOVEBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type LMPOPBuilder struct {
}

func (b *LMPOPBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	nk, nkErr := strconv.ParseInt(params[0], 10, 64)
	if nkErr != nil {
		return
	}

	rv := reflect.ValueOf(client.B().Lmpop().Numkeys(nk))

	params = params[1:]

	for _, param := range params {
		if key, has := strings.CutPrefix(param, "KEY:"); has {
			rv = rv.MethodByName("Key").Call([]reflect.Value{reflect.ValueOf(key)})[0]
			continue
		}
		if param == "LEFT" {
			rv = rv.MethodByName("Left").Call([]reflect.Value{})[0]
			continue
		}
		if param == "RIGHT" {
			rv = rv.MethodByName("Right").Call([]reflect.Value{})[0]
			continue
		}
		if count, has := strings.CutPrefix(param, "COUNT:"); has {
			vv, vvErr := strconv.ParseInt(count, 10, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Count").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			continue
		}
	}

	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)

	ok = true
	return
}

func (b *LMPOPBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type LPOPBuilder struct {
}

func (b *LPOPBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	rv := reflect.ValueOf(client.B().Lpop().Key(params[0]))
	params = params[1:]
	for _, param := range params {
		if count, has := strings.CutPrefix(param, "COUNT:"); has {
			vv, vvErr := strconv.ParseInt(count, 10, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Count").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			continue
		}
	}

	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)

	ok = true
	return
}

func (b *LPOPBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type LPOSBuilder struct {
}

func (b *LPOSBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	rv := reflect.ValueOf(client.B().Lpos().Key(params[0]).Element(params[1]))
	params = params[2:]
	for _, param := range params {
		if rank, has := strings.CutPrefix(param, "RANK:"); has {
			vv, vvErr := strconv.ParseInt(rank, 10, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Rank").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			continue
		}
		if count, has := strings.CutPrefix(param, "COUNT:"); has {
			vv, vvErr := strconv.ParseInt(count, 10, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Count").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			continue
		}
		if maxLen, has := strings.CutPrefix(param, "MAXLEN:"); has {
			vv, vvErr := strconv.ParseInt(maxLen, 10, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Maxlen").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			continue
		}
	}

	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)

	ok = true
	return
}

func (b *LPOSBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	rv := reflect.ValueOf(client.B().Lpos().Key(params[0]).Element(params[1]))
	params = params[2:]
	for _, param := range params {
		if rank, has := strings.CutPrefix(param, "RANK:"); has {
			vv, vvErr := strconv.ParseInt(rank, 10, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Rank").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			continue
		}
		if count, has := strings.CutPrefix(param, "COUNT:"); has {
			vv, vvErr := strconv.ParseInt(count, 10, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Count").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			continue
		}
		if maxLen, has := strings.CutPrefix(param, "MAXLEN:"); has {
			vv, vvErr := strconv.ParseInt(maxLen, 10, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Maxlen").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			continue
		}
	}

	rv = rv.MethodByName("Cache").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Cacheable)
	ok = true
	return
}

type LPUSHBuilder struct {
}

func (b *LPUSHBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Lpush().Key(params[0]).Element(params[1:]...).Build()
	ok = true
	return
}

func (b *LPUSHBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type LPUSHXBuilder struct {
}

func (b *LPUSHXBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Lpushx().Key(params[0]).Element(params[1:]...).Build()
	ok = true
	return
}

func (b *LPUSHXBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type LRANGEBuilder struct {
}

func (b *LRANGEBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	start, startErr := strconv.ParseInt(params[1], 10, 64)
	if startErr != nil {
		return
	}
	stop, stopErr := strconv.ParseInt(params[2], 10, 64)
	if stopErr != nil {
		return
	}
	v = client.B().Lrange().Key(params[0]).Start(start).Stop(stop).Build()
	ok = true
	return
}

func (b *LRANGEBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	start, startErr := strconv.ParseInt(params[1], 10, 64)
	if startErr != nil {
		return
	}
	stop, stopErr := strconv.ParseInt(params[2], 10, 64)
	if stopErr != nil {
		return
	}
	v = client.B().Lrange().Key(params[0]).Start(start).Stop(stop).Cache()
	ok = true
	return
}

type LREMBuilder struct {
}

func (b *LREMBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	count, countErr := strconv.ParseInt(params[1], 10, 64)
	if countErr != nil {
		return
	}
	v = client.B().Lrem().Key(params[0]).Count(count).Element(params[2]).Build()
	ok = true
	return
}

func (b *LREMBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type LSETBuilder struct {
}

func (b *LSETBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	index, indexErr := strconv.ParseInt(params[1], 10, 64)
	if indexErr != nil {
		return
	}
	v = client.B().Lset().Key(params[0]).Index(index).Element(params[2]).Build()
	ok = true
	return
}

func (b *LSETBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type LTRIMBuilder struct {
}

func (b *LTRIMBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	start, startErr := strconv.ParseInt(params[1], 10, 64)
	if startErr != nil {
		return
	}
	stop, stopErr := strconv.ParseInt(params[2], 10, 64)
	if stopErr != nil {
		return
	}
	v = client.B().Ltrim().Key(params[0]).Start(start).Stop(stop).Build()
	ok = true
	return
}

func (b *LTRIMBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type RPOPBuilder struct {
}

func (b *RPOPBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	rv := reflect.ValueOf(client.B().Rpop().Key(params[0]))
	params = params[1:]
	for _, param := range params {
		if count, has := strings.CutPrefix(param, "COUNT:"); has {
			vv, vvErr := strconv.ParseInt(count, 10, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Count").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			continue
		}
	}

	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)

	ok = true
	return
}

func (b *RPOPBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type RPUSHBuilder struct {
}

func (b *RPUSHBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Rpush().Key(params[0]).Element(params[1:]...).Build()
	ok = true
	return
}

func (b *RPUSHBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type RPUSHXBuilder struct {
}

func (b *RPUSHXBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Rpushx().Key(params[0]).Element(params[1:]...).Build()
	ok = true
	return
}

func (b *RPUSHXBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}
