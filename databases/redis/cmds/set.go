package cmds

import (
	"github.com/redis/rueidis"
	"reflect"
	"strconv"
	"strings"
)

const (
	SADD        = "SADD"
	SCARD       = "SCARD"
	SDIFF       = "SDIFF"
	SDIFFSTORE  = "SDIFFSTORE"
	SINTER      = "SINTER"
	SINTERCARD  = "SINTERCARD"
	SINTERSTORE = "SINTERSTORE"
	SISMEMBER   = "SISMEMBER"
	SMEMBERS    = "SMEMBERS"
	SMISMEMBER  = "SMISMEMBER"
	SMOVE       = "SMOVE"
	SPOP        = "SPOP"
	SRANDMEMBER = "SRANDMEMBER"
	SREM        = "SREM"
	SSCAN       = "SSCAN"
	SUNION      = "SUNION"
	SUNIONSTORE = "SUNIONSTORE"
)

func registerSet() {
	builders[SADD] = &SADDBuilder{}
	builders[SCARD] = &SCARDBuilder{}
	builders[SDIFF] = &SDIFFBuilder{}
	builders[SDIFFSTORE] = &SDIFFSTOREBuilder{}
	builders[SINTER] = &SINTERBuilder{}
	builders[SINTERCARD] = &SINTERCARDBuilder{}
	builders[SINTERSTORE] = &SINTERSTOREBuilder{}
	builders[SISMEMBER] = &SISMEMBERBuilder{}
	builders[SMEMBERS] = &SMEMBERSBuilder{}
	builders[SMISMEMBER] = &SMISMEMBERBuilder{}
	builders[SMOVE] = &SMOVEBuilder{}
	builders[SPOP] = &SPOPBuilder{}
	builders[SRANDMEMBER] = &SRANDMEMBERBuilder{}
	builders[SREM] = &SREMBuilder{}
	builders[SSCAN] = &SSCANBuilder{}
	builders[SUNION] = &SUNIONBuilder{}
	builders[SUNIONSTORE] = &SUNIONSTOREBuilder{}
}

type SADDBuilder struct {
}

func (b *SADDBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Sadd().Key(params[0]).Member(params[1:]...).Build()
	ok = true
	return
}

func (b *SADDBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type SCARDBuilder struct {
}

func (b *SCARDBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Scard().Key(params[0]).Build()
	ok = true
	return
}

func (b *SCARDBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Scard().Key(params[0]).Cache()
	ok = true
	return
}

type SDIFFBuilder struct {
}

func (b *SDIFFBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Sdiff().Key(params...).Build()
	ok = true
	return
}

func (b *SDIFFBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type SDIFFSTOREBuilder struct {
}

func (b *SDIFFSTOREBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Sdiffstore().Destination(params[0]).Key(params[1:]...).Build()
	ok = true
	return
}

func (b *SDIFFSTOREBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type SINTERBuilder struct {
}

func (b *SINTERBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Sinter().Key(params...).Build()
	ok = true
	return
}

func (b *SINTERBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type SINTERCARDBuilder struct {
}

func (b *SINTERCARDBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	nk, nkErr := strconv.ParseInt(params[0], 10, 64)
	if nkErr != nil {
		return
	}
	rv := reflect.ValueOf(client.B().Sintercard().Numkeys(nk))
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

func (b *SINTERCARDBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type SINTERSTOREBuilder struct {
}

func (b *SINTERSTOREBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Sinterstore().Destination(params[0]).Key(params[1:]...).Build()
	ok = true
	return
}

func (b *SINTERSTOREBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type SISMEMBERBuilder struct {
}

func (b *SISMEMBERBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Sismember().Key(params[0]).Member(params[1]).Build()
	ok = true
	return
}

func (b *SISMEMBERBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Sismember().Key(params[0]).Member(params[1]).Cache()
	ok = true
	return
}

type SMEMBERSBuilder struct {
}

func (b *SMEMBERSBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Smembers().Key(params[0]).Build()
	ok = true
	return
}

func (b *SMEMBERSBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Smembers().Key(params[0]).Cache()
	ok = true
	return
}

type SMISMEMBERBuilder struct {
}

func (b *SMISMEMBERBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Smismember().Key(params[0]).Member(params[1:]...).Build()
	ok = true
	return
}

func (b *SMISMEMBERBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Smismember().Key(params[0]).Member(params[1:]...).Cache()
	ok = true
	return
}

type SMOVEBuilder struct {
}

func (b *SMOVEBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Smove().Source(params[0]).Destination(params[1]).Member(params[2]).Build()
	ok = true
	return
}

func (b *SMOVEBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type SPOPBuilder struct {
}

func (b *SPOPBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	rv := reflect.ValueOf(client.B().Spop().Key(params[0]))
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

func (b *SPOPBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type SRANDMEMBERBuilder struct {
}

func (b *SRANDMEMBERBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	rv := reflect.ValueOf(client.B().Srandmember().Key(params[0]))
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

func (b *SRANDMEMBERBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type SREMBuilder struct {
}

func (b *SREMBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Srem().Key(params[0]).Member(params[1:]...).Build()
	ok = true
	return
}

func (b *SREMBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type SSCANBuilder struct {
}

func (b *SSCANBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	cursor, cursorErr := strconv.ParseUint(params[1], 10, 64)
	if cursorErr != nil {
		return
	}
	rv := reflect.ValueOf(client.B().Sscan().Key(params[0]).Cursor(cursor))
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

func (b *SSCANBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type SUNIONBuilder struct {
}

func (b *SUNIONBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Sunion().Key(params...).Build()
	ok = true
	return
}

func (b *SUNIONBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type SUNIONSTOREBuilder struct {
}

func (b *SUNIONSTOREBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Sunionstore().Destination(params[0]).Key(params[1:]...).Build()
	ok = true
	return
}

func (b *SUNIONSTOREBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}
