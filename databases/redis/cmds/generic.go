package cmds

import (
	"github.com/redis/rueidis"
	"strconv"
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
	WAIT      = "WAIT"
	WAITAOF   = "WAITAOF"
)

func registerGeneric() {
	builders[COPY] = &CopyBuilder{}
	builders[KEYS] = &KeysBuilder{}
	builders[DEL] = &DelBuilder{}
	builders[EXIST] = &ExistBuilder{}
	builders[EXPIRE] = &ExpireBuilder{}

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
