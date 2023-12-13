package cmds

import (
	"github.com/redis/rueidis"
	"reflect"
	"strconv"
	"strings"
)

// generic
const (
	COPY      = "COPY"
	KEYS      = "KEYS"
	DEL       = "DEL"
	EXIST     = "EXIST"
	EXPIRE    = "EXPIRE"
	EXPIREAT  = "EXPIREAT"
	TTL       = "TTL"
	PEXPIRE   = "PEXPIRE"
	PEXPIREAT = "PEXPIREAT"
	PTTL      = "PTTL"
	PERSIST   = "PERSIST"
	RENAME    = "RENAME"
	RENAMENX  = "RENAMENX"
	SCAN      = "SCAN"
	SORT      = "SORT"
	SORTRO    = "SORT_RO"
	TOUCH     = "TOUCH"
	UNLINKS   = "UNLINKS"
)

func registerGeneric() {
	builders[COPY] = &CopyBuilder{}
	builders[KEYS] = &KeysBuilder{}
	builders[DEL] = &DelBuilder{}
	builders[EXIST] = &ExistBuilder{}
	builders[EXPIRE] = &ExpireBuilder{}
	builders[EXPIREAT] = &ExpireAtBuilder{}
	builders[TTL] = &TTLBuilder{}
	builders[PEXPIRE] = &PExpireBuilder{}
	builders[PEXPIREAT] = &PExpireAtBuilder{}
	builders[PTTL] = &PTTLBuilder{}
	builders[PERSIST] = &PersistBuilder{}
	builders[RENAME] = &RenameBuilder{}
	builders[RENAMENX] = &RenameNxBuilder{}
	builders[SCAN] = &ScanBuilder{}
	builders[SORT] = &SortBuilder{}
	builders[SORTRO] = &SortRoBuilder{}
	builders[TOUCH] = &TouchBuilder{}
	builders[UNLINKS] = &UnlinksBuilder{}
}

type CopyBuilder struct {
}

func (b *CopyBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Copy().Source(params[0]).Destination(params[1]).Build()
	ok = true
	return
}

func (b *CopyBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type KeysBuilder struct {
}

func (b *KeysBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Keys().Pattern(params[0]).Build()
	ok = true
	return
}

func (b *KeysBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type DelBuilder struct {
}

func (b *DelBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Del().Key(params...).Build()
	ok = true
	return
}

func (b *DelBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ExistBuilder struct {
}

func (b *ExistBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Exists().Key(params...).Build()
	ok = true
	return
}

func (b *ExistBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ExpireBuilder struct {
}

func (b *ExpireBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	sec, secErr := strconv.ParseInt(params[1], 10, 64)
	if secErr != nil {
		return
	}
	v = client.B().Expire().Key(params[0]).Seconds(sec).Build()
	ok = true
	return
}

func (b *ExpireBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ExpireAtBuilder struct {
}

func (b *ExpireAtBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	timestamp, timestampErr := strconv.ParseInt(params[1], 10, 64)
	if timestampErr != nil {
		return
	}
	v = client.B().Expireat().Key(params[0]).Timestamp(timestamp).Build()
	ok = true
	return
}

func (b *ExpireAtBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type TTLBuilder struct {
}

func (b *TTLBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Ttl().Key(params[0]).Build()
	ok = true
	return
}

func (b *TTLBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Ttl().Key(params[0]).Cache()
	ok = true
	return
}

type PExpireBuilder struct {
}

func (b *PExpireBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	sec, secErr := strconv.ParseInt(params[1], 10, 64)
	if secErr != nil {
		return
	}
	v = client.B().Pexpire().Key(params[0]).Milliseconds(sec).Build()
	ok = true
	return
}

func (b *PExpireBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type PExpireAtBuilder struct {
}

func (b *PExpireAtBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	timestamp, timestampErr := strconv.ParseInt(params[1], 10, 64)
	if timestampErr != nil {
		return
	}
	v = client.B().Pexpireat().Key(params[0]).MillisecondsTimestamp(timestamp).Build()
	ok = true
	return
}

func (b *PExpireAtBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type PTTLBuilder struct {
}

func (b *PTTLBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Pttl().Key(params[0]).Build()
	ok = true
	return
}

func (b *PTTLBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	v = client.B().Pttl().Key(params[0]).Cache()
	ok = true
	return
}

type PersistBuilder struct {
}

func (b *PersistBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Persist().Key(params[0]).Build()
	ok = true
	return
}

func (b *PersistBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type RenameBuilder struct {
}

func (b *RenameBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Rename().Key(params[0]).Newkey(params[1]).Build()
	ok = true
	return
}

func (b *RenameBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type RenameNxBuilder struct {
}

func (b *RenameNxBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Renamenx().Key(params[0]).Newkey(params[1]).Build()
	ok = true
	return
}

func (b *RenameNxBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type ScanBuilder struct {
}

func (b *ScanBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	cursor, cursorErr := strconv.ParseUint(params[0], 10, 64)
	if cursorErr != nil {
		return
	}

	rv := reflect.ValueOf(client.B().Scan().Cursor(cursor))
	for _, param := range params {
		match, hasMatch := strings.CutPrefix(param, "MATCH:")
		if hasMatch {
			rv = rv.MethodByName("Match").Call([]reflect.Value{reflect.ValueOf(match)})[0]
			continue
		}
		count, hasCount := strings.CutPrefix(param, "COUNT:")
		if hasCount {
			vv, vvErr := strconv.ParseInt(count, 10, 64)
			if vvErr != nil {
				return
			}
			rv = rv.MethodByName("Count").Call([]reflect.Value{reflect.ValueOf(vv)})[0]
			continue
		}
		typ, hasType := strings.CutPrefix(param, "TYPE:")
		if hasType {
			rv = rv.MethodByName("Type").Call([]reflect.Value{reflect.ValueOf(typ)})[0]
			continue
		}
	}

	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)
	ok = true
	return
}

func (b *ScanBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type SortBuilder struct {
}

func (b *SortBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	key := params[0]
	params = params[1:]
	if len(params) == 0 {
		v = client.B().Sort().Key(key).Build()
		ok = true
		return
	}
	by := ""
	for _, param := range params {
		match, has := strings.CutPrefix(param, "BY:")
		if has {
			by = match
			break
		}
	}
	offset := int64(0)
	limit := int64(0)
	for _, param := range params {
		limits, has := strings.CutPrefix(param, "LIMIT:")
		if has {
			items := strings.Split(limits, ",")
			if len(items) != 2 {
				return
			}
			var limitsErr error
			offset, limitsErr = strconv.ParseInt(items[0], 10, 64)
			if limitsErr != nil {
				return
			}
			limit, limitsErr = strconv.ParseInt(items[1], 10, 64)
			if limitsErr != nil {
				return
			}
			break
		}
	}
	gets := make([]string, 0, 1)
	for _, param := range params {
		get, has := strings.CutPrefix(param, "GET:")
		if has {
			gets = append(gets, get)
		}
	}
	order := ""
	for _, param := range params {
		ov, has := strings.CutPrefix(param, "ORDER:")
		if has {
			order = strings.ToLower(ov)
			break
		}
	}
	alpha := false
	for _, param := range params {
		if param == "ALPHA" {
			alpha = true
			break
		}
	}
	store := ""
	for _, param := range params {
		sv, has := strings.CutPrefix(param, "STORE:")
		if has {
			store = sv
			break
		}
	}
	rv := reflect.ValueOf(client.B().Sort().Key(key))
	if by != "" {
		rv = rv.MethodByName("By").Call([]reflect.Value{reflect.ValueOf(by)})[0]
	}
	if limit > 0 {
		rv = rv.MethodByName("Limit").Call([]reflect.Value{reflect.ValueOf(offset), reflect.ValueOf(limit)})[0]
	}
	if len(gets) > 0 {
		rv = rv.MethodByName("Get").Call([]reflect.Value{})[0]
		for _, get := range gets {
			rv = rv.MethodByName("Get").Call([]reflect.Value{reflect.ValueOf(get)})[0]
		}
	}
	if order == "asc" {
		rv = rv.MethodByName("Asc").Call([]reflect.Value{})[0]
	} else if order == "desc" {
		rv = rv.MethodByName("Desc").Call([]reflect.Value{})[0]
	}
	if alpha {
		rv = rv.MethodByName("Alpha").Call([]reflect.Value{})[0]
	}
	if store != "" {
		rv = rv.MethodByName("Store").Call([]reflect.Value{reflect.ValueOf(store)})[0]
	}
	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)
	ok = true
	return
}

func (b *SortBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type SortRoBuilder struct {
}

func (b *SortRoBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	key := params[0]
	params = params[1:]
	if len(params) == 0 {
		v = client.B().Sort().Key(key).Build()
		ok = true
		return
	}
	by := ""
	for _, param := range params {
		match, has := strings.CutPrefix(param, "BY:")
		if has {
			by = match
			break
		}
	}
	offset := int64(0)
	limit := int64(0)
	for _, param := range params {
		limits, has := strings.CutPrefix(param, "LIMIT:")
		if has {
			items := strings.Split(limits, ",")
			if len(items) != 2 {
				return
			}
			var limitsErr error
			offset, limitsErr = strconv.ParseInt(items[0], 10, 64)
			if limitsErr != nil {
				return
			}
			limit, limitsErr = strconv.ParseInt(items[1], 10, 64)
			if limitsErr != nil {
				return
			}
			break
		}
	}
	gets := make([]string, 0, 1)
	for _, param := range params {
		get, has := strings.CutPrefix(param, "GET:")
		if has {
			gets = append(gets, get)
		}
	}
	order := ""
	for _, param := range params {
		ov, has := strings.CutPrefix(param, "ORDER:")
		if has {
			order = ov
			break
		}
	}
	alpha := false
	for _, param := range params {
		if param == "ALPHA" {
			alpha = true
			break
		}
	}
	rv := reflect.ValueOf(client.B().SortRo().Key(key))
	if by != "" {
		rv = rv.MethodByName("By").Call([]reflect.Value{reflect.ValueOf(by)})[0]
	}
	if limit > 0 {
		rv = rv.MethodByName("Limit").Call([]reflect.Value{reflect.ValueOf(offset), reflect.ValueOf(limit)})[0]
	}
	for _, get := range gets {
		rv = rv.MethodByName("Get").Call([]reflect.Value{reflect.ValueOf(get)})[0]
	}
	if order == "ASC" {
		rv = rv.MethodByName("Asc").Call([]reflect.Value{})[0]
	} else if order == "DESC" {
		rv = rv.MethodByName("Desc").Call([]reflect.Value{})[0]
	}
	if alpha {
		rv = rv.MethodByName("Alpha").Call([]reflect.Value{})[0]
	}
	rv = rv.MethodByName("Build").Call([]reflect.Value{})[0]
	v = rv.Interface().(rueidis.Completed)
	ok = true
	return
}

func (b *SortRoBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type TouchBuilder struct {
}

func (b *TouchBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Touch().Key(params[0]).Build()
	ok = true
	return
}

func (b *TouchBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}

type UnlinksBuilder struct {
}

func (b *UnlinksBuilder) Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool) {
	v = client.B().Unlink().Key(params...).Build()
	ok = true
	return
}

func (b *UnlinksBuilder) Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool) {
	return
}
