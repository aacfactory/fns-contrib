package cmds

import (
	"github.com/redis/rueidis"
	"reflect"
	"strconv"
	"strings"
)

const (
	BZMPOP           = "BZMPOP"
	BZPOPMAX         = "BZPOPMAX"
	BZPOPMIN         = "BZPOPMIN"
	ZADD             = "ZADD"
	ZCARD            = "ZCARD"
	ZCOUNT           = "ZCOUNT"
	ZDIFF            = "ZDIFF"
	ZDIFFSTORE       = "ZDIFFSTORE"
	ZINCRBY          = "ZINCRBY"
	ZINTER           = "ZINTER"
	ZINTERCARD       = "ZINTERCARD"
	ZINTERSTORE      = "ZINTERSTORE"
	ZLEXCOUNT        = "ZLEXCOUNT"
	ZMPOP            = "ZMPOP"
	ZMSCORE          = "ZMSCORE"
	ZPOPMAX          = "ZPOPMAX"
	ZPOPMIN          = "ZPOPMIN"
	ZRANDMEMBER      = "ZRANDMEMBER"
	ZRANGE           = "ZRANGE"
	ZRANGESTORE      = "ZRANGESTORE"
	ZRANK            = "ZRANK"
	ZREM             = "ZREM"
	ZREMRANGEBYLEX   = "ZREMRANGEBYLEX"
	ZREMRANGEBYRANK  = "ZREMRANGEBYRANK"
	ZREMRANGEBYSCORE = "ZREMRANGEBYSCORE"
	ZREVRANK         = "ZREVRANK"
	ZSCAN            = "ZSCAN"
	ZSCORE           = "ZSCORE"
	ZUNION           = "ZUNION"
	ZUNIONSTORE      = "ZUNIONSTORE"
)

func registerSortedSet() {
	builders[BZMPOP] = &BZMPOPBuilder{}
	builders[BZPOPMAX] = &BZPOPMAXBuilder{}
	builders[BZPOPMIN] = &BZPOPMINBuilder{}
	builders[ZADD] = &ZADDBuilder{}
	builders[ZCARD] = &ZCARDBuilder{}
	builders[ZCOUNT] = &ZCOUNTBuilder{}
	builders[ZDIFF] = &ZDIFFBuilder{}
	builders[ZDIFFSTORE] = &ZDIFFSTOREBuilder{}
	builders[ZINCRBY] = &ZINCRBYBuilder{}
	builders[ZINTER] = &ZINTERBuilder{}
	builders[ZINTERCARD] = &ZINTERCARDBuilder{}
	builders[ZINTERSTORE] = &ZINTERSTOREBuilder{}
	builders[ZLEXCOUNT] = &ZLEXCOUNTBuilder{}
	builders[ZMPOP] = &ZMPOPBuilder{}
	builders[ZMSCORE] = &ZMSCOREBuilder{}
	builders[ZPOPMAX] = &ZPOPMAXBuilder{}
	builders[ZPOPMIN] = &ZPOPMINBuilder{}
	builders[ZRANDMEMBER] = &ZRANDMEMBERBuilder{}
	builders[ZRANGE] = &ZRANGEBuilder{}
	builders[ZRANGESTORE] = &ZRANGESTOREBuilder{}
	builders[ZRANK] = &ZRANKBuilder{}
	builders[ZREM] = &ZREMBuilder{}
	builders[ZREMRANGEBYLEX] = &ZREMRANGEBYLEXBuilder{}
	builders[ZREMRANGEBYRANK] = &ZREMRANGEBYRANKBuilder{}
	builders[ZREMRANGEBYSCORE] = &ZREMRANGEBYSCOREBuilder{}
	builders[ZREVRANK] = &ZREVRANKBuilder{}
	builders[ZSCAN] = &ZSCANBuilder{}
	builders[ZSCORE] = &ZSCOREBuilder{}
	builders[ZUNION] = &ZUNIONBuilder{}
	builders[ZUNIONSTORE] = &ZUNIONSTOREBuilder{}
}

type BZMPOPBuilder struct {
}

func (b *BZMPOPBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	timeout, timeoutErr := strconv.ParseFloat(params[0], 64)
	if timeoutErr != nil {
		return
	}
	nk, nkErr := strconv.ParseInt(params[1], 10, 64)
	if nkErr != nil {
		return
	}
	rv := reflect.ValueOf(client.B().Bzmpop().Timeout(timeout).Numkeys(nk))
	params = params[2:]
	for _, param := range params {
		if key, has := strings.CutPrefix(param, "KEY:"); has {
			rv = rv.MethodByName("Key").Call([]reflect.Value{reflect.ValueOf(key)})[0]
			continue
		}
		if param == "MIN" {
			rv = rv.MethodByName("Min").Call([]reflect.Value{})[0]
			continue
		}
		if param == "MAX" {
			rv = rv.MethodByName("Max").Call([]reflect.Value{})[0]
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

func (b *BZMPOPBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type BZPOPMAXBuilder struct {
}

func (b *BZPOPMAXBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	timeout, timeoutErr := strconv.ParseFloat(params[0], 64)
	if timeoutErr != nil {
		return
	}
	v = client.B().Bzpopmax().Key(params[1:]...).Timeout(timeout).Build()
	ok = true
	return
}

func (b *BZPOPMAXBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type BZPOPMINBuilder struct {
}

func (b *BZPOPMINBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	timeout, timeoutErr := strconv.ParseFloat(params[0], 64)
	if timeoutErr != nil {
		return
	}
	v = client.B().Bzpopmin().Key(params[1:]...).Timeout(timeout).Build()
	ok = true
	return
}

func (b *BZPOPMINBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ZADDBuilder struct {
}

func (b *ZADDBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	rv := reflect.ValueOf(client.B().Zadd().Key(params[0]))
	params = params[1:]
	sm := 0
	for _, param := range params {
		if param == "NX" {
			rv = rv.MethodByName("Nx").Call([]reflect.Value{})[0]
			continue
		}
		if param == "XX" {
			rv = rv.MethodByName("Xx").Call([]reflect.Value{})[0]
			continue
		}
		if param == "GT" {
			rv = rv.MethodByName("Gt").Call([]reflect.Value{})[0]
			continue
		}
		if param == "LT" {
			rv = rv.MethodByName("Lt").Call([]reflect.Value{})[0]
			continue
		}
		if param == "CH" {
			rv = rv.MethodByName("Ch").Call([]reflect.Value{})[0]
			continue
		}
		if param == "INCR" {
			rv = rv.MethodByName("Incr").Call([]reflect.Value{})[0]
			continue
		}
		if vv, has := strings.CutPrefix(param, "SCORE:"); has {
			if sm == 0 {
				rv = rv.MethodByName("ScoreMember").Call([]reflect.Value{})[0]
			}
			idx := strings.IndexByte(vv, '+')
			score, scoreErr := strconv.ParseFloat(vv[0:idx], 64)
			if scoreErr != nil {
				return
			}
			member := vv[idx+1:]
			rv = rv.MethodByName("ScoreMember").Call([]reflect.Value{reflect.ValueOf(score), reflect.ValueOf(member)})[0]
			sm++
		}
	}

	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)
	ok = true
	return
}

func (b *ZADDBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ZCARDBuilder struct {
}

func (b *ZCARDBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Zcard().Key(params[0]).Build()
	ok = true
	return
}

func (b *ZCARDBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Zcard().Key(params[0]).Cache()
	ok = true
	return
}

type ZCOUNTBuilder struct {
}

func (b *ZCOUNTBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Zcount().Key(params[0]).Min(params[1]).Max(params[2]).Build()
	ok = true
	return
}

func (b *ZCOUNTBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Zcount().Key(params[0]).Min(params[1]).Max(params[2]).Cache()
	ok = true
	return
}

type ZDIFFBuilder struct {
}

func (b *ZDIFFBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	nk, nkErr := strconv.ParseInt(params[0], 10, 64)
	if nkErr != nil {
		return
	}
	rv := reflect.ValueOf(client.B().Zdiff().Numkeys(nk))

	params = params[1:]
	for _, param := range params {
		if key, has := strings.CutPrefix(param, "KEY:"); has {
			rv = rv.MethodByName("Key").Call([]reflect.Value{reflect.ValueOf(key)})[0]
			continue
		}
		if param == "WITHSCORES" {
			rv = rv.MethodByName("Withscores").Call([]reflect.Value{})[0]
			continue
		}
	}

	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)
	ok = true
	return
}

func (b *ZDIFFBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ZDIFFSTOREBuilder struct {
}

func (b *ZDIFFSTOREBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	nk, nkErr := strconv.ParseInt(params[1], 10, 64)
	if nkErr != nil {
		return
	}
	v = client.B().Zdiffstore().Destination(params[0]).Numkeys(nk).Key(params[2:]...).Build()
	ok = true
	return
}

func (b *ZDIFFSTOREBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ZINCRBYBuilder struct {
}

func (b *ZINCRBYBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	delta, deltaErr := strconv.ParseFloat(params[1], 64)
	if deltaErr != nil {
		return
	}
	v = client.B().Zincrby().Key(params[0]).Increment(delta).Member(params[2]).Build()
	ok = true
	return
}

func (b *ZINCRBYBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ZINTERBuilder struct {
}

func (b *ZINTERBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	nk, nkErr := strconv.ParseInt(params[0], 10, 64)
	if nkErr != nil {
		return
	}
	rv := reflect.ValueOf(client.B().Zinter().Numkeys(nk))
	params = params[1:]
	for _, param := range params {
		if key, has := strings.CutPrefix(param, "KEY:"); has {
			rv = rv.MethodByName("Key").Call([]reflect.Value{reflect.ValueOf(key)})[0]
			continue
		}
		if weight, has := strings.CutPrefix(param, "WEIGHT:"); has {
			vv, vvErr := strconv.ParseInt(weight, 10, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Weights").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			continue
		}
		if param == "AGGMAX" {
			rv = rv.MethodByName("AggregateMax").Call([]reflect.Value{})[0]
			continue
		}
		if param == "AGGMIX" {
			rv = rv.MethodByName("AggregateMin").Call([]reflect.Value{})[0]
			continue
		}
		if param == "AGGSUM" {
			rv = rv.MethodByName("AggregateSum").Call([]reflect.Value{})[0]
			continue
		}
		if param == "WITHSCORES" {
			rv = rv.MethodByName("Withscores").Call([]reflect.Value{})[0]
			continue
		}
	}
	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)
	ok = true
	return
}

func (b *ZINTERBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ZINTERCARDBuilder struct {
}

func (b *ZINTERCARDBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	nk, nkErr := strconv.ParseInt(params[0], 10, 64)
	if nkErr != nil {
		return
	}
	rv := reflect.ValueOf(client.B().Zintercard().Numkeys(nk))
	params = params[1:]
	for _, param := range params {
		if key, has := strings.CutPrefix(param, "KEY:"); has {
			rv = rv.MethodByName("Key").Call([]reflect.Value{reflect.ValueOf(key)})[0]
			continue
		}
		if limit, has := strings.CutPrefix(param, "LIMIT:"); has {
			vv, vvErr := strconv.ParseInt(limit, 10, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Limit").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			continue
		}
	}
	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)
	ok = true
	return
}

func (b *ZINTERCARDBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ZINTERSTOREBuilder struct {
}

func (b *ZINTERSTOREBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	nk, nkErr := strconv.ParseInt(params[1], 10, 64)
	if nkErr != nil {
		return
	}
	rv := reflect.ValueOf(client.B().Zinterstore().Destination(params[0]).Numkeys(nk))
	params = params[2:]
	for _, param := range params {
		if key, has := strings.CutPrefix(param, "KEY:"); has {
			rv = rv.MethodByName("Key").Call([]reflect.Value{reflect.ValueOf(key)})[0]
			continue
		}
		if weight, has := strings.CutPrefix(param, "WEIGHT:"); has {
			vv, vvErr := strconv.ParseInt(weight, 10, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Weights").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			continue
		}
		if param == "AGGMAX" {
			rv = rv.MethodByName("AggregateMax").Call([]reflect.Value{})[0]
			continue
		}
		if param == "AGGMIX" {
			rv = rv.MethodByName("AggregateMin").Call([]reflect.Value{})[0]
			continue
		}
		if param == "AGGSUM" {
			rv = rv.MethodByName("AggregateSum").Call([]reflect.Value{})[0]
			continue
		}
	}
	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)
	ok = true
	return
}

func (b *ZINTERSTOREBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ZLEXCOUNTBuilder struct {
}

func (b *ZLEXCOUNTBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Zlexcount().Key(params[0]).Min(params[1]).Max(params[2]).Build()
	ok = true
	return
}

func (b *ZLEXCOUNTBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Zlexcount().Key(params[0]).Min(params[1]).Max(params[2]).Cache()
	ok = true
	return
}

type ZMPOPBuilder struct {
}

func (b *ZMPOPBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	nk, nkErr := strconv.ParseInt(params[0], 10, 64)
	if nkErr != nil {
		return
	}
	rv := reflect.ValueOf(client.B().Zmpop().Numkeys(nk))
	params = params[1:]
	for _, param := range params {
		if key, has := strings.CutPrefix(param, "KEY:"); has {
			rv = rv.MethodByName("Key").Call([]reflect.Value{reflect.ValueOf(key)})[0]
			continue
		}
		if param == "MIN" {
			rv = rv.MethodByName("Max").Call([]reflect.Value{})[0]
			continue
		}
		if param == "MAZ" {
			rv = rv.MethodByName("Max").Call([]reflect.Value{})[0]
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

func (b *ZMPOPBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ZMSCOREBuilder struct {
}

func (b *ZMSCOREBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Zmscore().Key(params[0]).Member(params[1:]...).Build()
	ok = true
	return
}

func (b *ZMSCOREBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Zmscore().Key(params[0]).Member(params[1:]...).Cache()
	ok = true
	return
}

type ZPOPMAXBuilder struct {
}

func (b *ZPOPMAXBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	if len(params) == 1 {
		v = client.B().Zpopmax().Key(params[0]).Build()
	} else {
		count, countErr := strconv.ParseInt(params[1], 10, 64)
		if countErr != nil {
			return
		}
		v = client.B().Zpopmax().Key(params[0]).Count(count).Build()
	}
	ok = true
	return
}

func (b *ZPOPMAXBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ZPOPMINBuilder struct {
}

func (b *ZPOPMINBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	if len(params) == 1 {
		v = client.B().Zpopmin().Key(params[0]).Build()
	} else {
		count, countErr := strconv.ParseInt(params[1], 10, 64)
		if countErr != nil {
			return
		}
		v = client.B().Zpopmax().Key(params[0]).Count(count).Build()
	}
	ok = true
	return
}

func (b *ZPOPMINBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ZRANDMEMBERBuilder struct {
}

func (b *ZRANDMEMBERBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	if len(params) == 1 {
		v = client.B().Zrandmember().Key(params[0]).Build()
	} else if len(params) > 1 {
		count, countErr := strconv.ParseInt(params[1], 10, 64)
		if countErr != nil {
			return
		}
		if len(params) == 2 {
			v = client.B().Zrandmember().Key(params[0]).Count(count).Build()
		} else {
			v = client.B().Zrandmember().Key(params[0]).Count(count).Withscores().Build()
		}
	}
	ok = true
	return
}

func (b *ZRANDMEMBERBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ZRANGEBuilder struct {
}

func (b *ZRANGEBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	rv := reflect.ValueOf(client.B().Zrange().Key(params[0]).Min(params[1]).Max(params[2]))
	params = params[3:]
	for _, param := range params {
		if param == "BYSCORE" {
			rv = rv.MethodByName("Byscore").Call([]reflect.Value{})[0]
			continue
		}
		if param == "BYLEX" {
			rv = rv.MethodByName("Bylex").Call([]reflect.Value{})[0]
			continue
		}
		if param == "REV" {
			rv = rv.MethodByName("Rev").Call([]reflect.Value{})[0]
			continue
		}
		if vv, has := strings.CutPrefix(param, "LIMIT:"); has {
			idx := strings.IndexByte(vv, ',')
			offset, offsetErr := strconv.ParseInt(vv[0:idx], 10, 64)
			if offsetErr != nil {
				return
			}
			limit, limitErr := strconv.ParseInt(vv[idx+1:], 10, 64)
			if limitErr != nil {
				return
			}
			rv = rv.MethodByName("Limit").Call([]reflect.Value{reflect.ValueOf(offset), reflect.ValueOf(limit)})[0]
			continue
		}

		if param == "WITHSCORES" {
			rv = rv.MethodByName("Withscores").Call([]reflect.Value{})[0]
			continue
		}
	}
	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)
	ok = true
	return
}

func (b *ZRANGEBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	rv := reflect.ValueOf(client.B().Zrange().Key(params[0]).Min(params[1]).Max(params[2]))
	params = params[3:]
	for _, param := range params {
		if param == "BYSCORE" {
			rv = rv.MethodByName("Byscore").Call([]reflect.Value{})[0]
			continue
		}
		if param == "BYLEX" {
			rv = rv.MethodByName("Bylex").Call([]reflect.Value{})[0]
			continue
		}
		if param == "REV" {
			rv = rv.MethodByName("Rev").Call([]reflect.Value{})[0]
			continue
		}
		if vv, has := strings.CutPrefix(param, "LIMIT:"); has {
			idx := strings.IndexByte(vv, ',')
			offset, offsetErr := strconv.ParseInt(vv[0:idx], 10, 64)
			if offsetErr != nil {
				return
			}
			limit, limitErr := strconv.ParseInt(vv[idx+1:], 10, 64)
			if limitErr != nil {
				return
			}
			rv = rv.MethodByName("Limit").Call([]reflect.Value{reflect.ValueOf(offset), reflect.ValueOf(limit)})[0]
			continue
		}

		if param == "WITHSCORES" {
			rv = rv.MethodByName("Withscores").Call([]reflect.Value{})[0]
			continue
		}
	}
	rv = rv.MethodByName("Cache").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Cacheable)
	ok = true
	return
}

type ZRANGESTOREBuilder struct {
}

func (b *ZRANGESTOREBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	rv := reflect.ValueOf(client.B().Zrangestore().Dst(params[0]).Src(params[1]).Min(params[2]).Max(params[3]))
	params = params[4:]
	for _, param := range params {
		if param == "BYSCORE" {
			rv = rv.MethodByName("Byscore").Call([]reflect.Value{})[0]
			continue
		}
		if param == "BYLEX" {
			rv = rv.MethodByName("Bylex").Call([]reflect.Value{})[0]
			continue
		}
		if param == "REV" {
			rv = rv.MethodByName("Rev").Call([]reflect.Value{})[0]
			continue
		}
		if vv, has := strings.CutPrefix(param, "LIMIT:"); has {
			idx := strings.IndexByte(vv, ',')
			offset, offsetErr := strconv.ParseInt(vv[0:idx], 10, 64)
			if offsetErr != nil {
				return
			}
			limit, limitErr := strconv.ParseInt(vv[idx+1:], 10, 64)
			if limitErr != nil {
				return
			}
			rv = rv.MethodByName("Limit").Call([]reflect.Value{reflect.ValueOf(offset), reflect.ValueOf(limit)})[0]
			continue
		}
	}
	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)
	ok = true
	return
}

func (b *ZRANGESTOREBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ZRANKBuilder struct {
}

func (b *ZRANKBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	if len(params) < 3 {
		v = client.B().Zrank().Key(params[0]).Member(params[1]).Build()
	} else {
		v = client.B().Zrank().Key(params[0]).Member(params[1]).Withscore().Build()
	}
	ok = true
	return
}

func (b *ZRANKBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	if len(params) < 3 {
		v = client.B().Zrank().Key(params[0]).Member(params[1]).Cache()
	} else {
		v = client.B().Zrank().Key(params[0]).Member(params[1]).Withscore().Cache()
	}
	ok = true
	return
}

type ZREMBuilder struct {
}

func (b *ZREMBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Zrem().Key(params[0]).Member(params[1:]...).Build()
	ok = true
	return
}

func (b *ZREMBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ZREMRANGEBYLEXBuilder struct {
}

func (b *ZREMRANGEBYLEXBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Zremrangebylex().Key(params[0]).Min(params[1]).Max(params[2]).Build()
	ok = true
	return
}

func (b *ZREMRANGEBYLEXBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ZREMRANGEBYRANKBuilder struct {
}

func (b *ZREMRANGEBYRANKBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	start, startErr := strconv.ParseInt(params[1], 10, 64)
	if startErr != nil {
		return
	}
	stop, stopErr := strconv.ParseInt(params[2], 10, 64)
	if stopErr != nil {
		return
	}
	v = client.B().Zremrangebyrank().Key(params[0]).Start(start).Stop(stop).Build()
	ok = true
	return
}

func (b *ZREMRANGEBYRANKBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ZREMRANGEBYSCOREBuilder struct {
}

func (b *ZREMRANGEBYSCOREBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Zremrangebyscore().Key(params[0]).Min(params[1]).Max(params[2]).Build()
	ok = true
	return
}

func (b *ZREMRANGEBYSCOREBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ZREVRANKBuilder struct {
}

func (b *ZREVRANKBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	if len(params) < 3 {
		v = client.B().Zrevrank().Key(params[0]).Member(params[1]).Build()
	} else {
		v = client.B().Zrevrank().Key(params[0]).Member(params[1]).Withscore().Build()
	}
	ok = true
	return
}

func (b *ZREVRANKBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	if len(params) < 3 {
		v = client.B().Zrevrank().Key(params[0]).Member(params[1]).Cache()
	} else {
		v = client.B().Zrevrank().Key(params[0]).Member(params[1]).Withscore().Cache()
	}
	ok = true
	return
}

type ZSCANBuilder struct {
}

func (b *ZSCANBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	cursor, cursorErr := strconv.ParseUint(params[1], 10, 64)
	if cursorErr != nil {
		return
	}
	rv := reflect.ValueOf(client.B().Zscan().Key(params[0]).Cursor(cursor))
	params = params[2:]
	for _, param := range params {
		if pattern, has := strings.CutPrefix(param, "MATCH:"); has {
			rv = rv.MethodByName("Match").Call([]reflect.Value{reflect.ValueOf(pattern)})[0]
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

func (b *ZSCANBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ZSCOREBuilder struct {
}

func (b *ZSCOREBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Zscore().Key(params[0]).Member(params[1]).Build()
	ok = true
	return
}

func (b *ZSCOREBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Zscore().Key(params[0]).Member(params[1]).Cache()
	ok = true
	return
}

type ZUNIONBuilder struct {
}

func (b *ZUNIONBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	nk, nkErr := strconv.ParseInt(params[0], 10, 64)
	if nkErr != nil {
		return
	}
	rv := reflect.ValueOf(client.B().Zunion().Numkeys(nk))
	params = params[1:]
	for _, param := range params {
		if key, has := strings.CutPrefix(param, "KEY:"); has {
			rv = rv.MethodByName("Key").Call([]reflect.Value{reflect.ValueOf(key)})[0]
			continue
		}
		if weight, has := strings.CutPrefix(param, "WEIGHT:"); has {
			vv, vvErr := strconv.ParseInt(weight, 10, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Weights").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			continue
		}
		if param == "AGGMAX" {
			rv = rv.MethodByName("AggregateMax").Call([]reflect.Value{})[0]
			continue
		}
		if param == "AGGMIX" {
			rv = rv.MethodByName("AggregateMin").Call([]reflect.Value{})[0]
			continue
		}
		if param == "AGGSUM" {
			rv = rv.MethodByName("AggregateSum").Call([]reflect.Value{})[0]
			continue
		}
		if param == "WITHSCORES" {
			rv = rv.MethodByName("Withscores").Call([]reflect.Value{})[0]
			continue
		}
	}
	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)
	ok = true
	return
}

func (b *ZUNIONBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ZUNIONSTOREBuilder struct {
}

func (b *ZUNIONSTOREBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	nk, nkErr := strconv.ParseInt(params[1], 10, 64)
	if nkErr != nil {
		return
	}
	rv := reflect.ValueOf(client.B().Zunionstore().Destination(params[0]).Numkeys(nk))
	params = params[1:]
	for _, param := range params {
		if key, has := strings.CutPrefix(param, "KEY:"); has {
			rv = rv.MethodByName("Key").Call([]reflect.Value{reflect.ValueOf(key)})[0]
			continue
		}
		if weight, has := strings.CutPrefix(param, "WEIGHT:"); has {
			vv, vvErr := strconv.ParseInt(weight, 10, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Weights").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			continue
		}
		if param == "AGGMAX" {
			rv = rv.MethodByName("AggregateMax").Call([]reflect.Value{})[0]
			continue
		}
		if param == "AGGMIX" {
			rv = rv.MethodByName("AggregateMin").Call([]reflect.Value{})[0]
			continue
		}
		if param == "AGGSUM" {
			rv = rv.MethodByName("AggregateSum").Call([]reflect.Value{})[0]
			continue
		}
	}
	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)
	ok = true
	return
}

func (b *ZUNIONSTOREBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}
