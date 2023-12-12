package redis

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/redis/cmds"
	"strconv"
	"time"
)

func Copy(src string, dst string) CopyBuilder {
	return CopyBuilder{
		params: []string{src, dst},
	}
}

type CopyBuilder struct {
	params []string
}

func (builder CopyBuilder) Build() (cmd Command) {
	cmd.Name = cmds.COPY
	cmd.Params = builder.params
	return
}

func Keys(key ...string) KeysBuilder {
	return KeysBuilder{
		params: key,
	}
}

type KeysBuilder struct {
	params []string
}

func (builder KeysBuilder) Build() (cmd Command) {
	cmd.Name = cmds.KEYS
	cmd.Params = builder.params
	return
}

func Del(key string) DelBuilder {
	return DelBuilder{
		params: []string{key},
	}
}

type DelBuilder struct {
	params []string
}

func (builder DelBuilder) Build() (cmd Command) {
	cmd.Name = cmds.DEL
	cmd.Params = builder.params
	return
}

func Exist(key ...string) ExistBuilder {
	return ExistBuilder{
		params: key,
	}
}

type ExistBuilder struct {
	params []string
}

func (builder ExistBuilder) Build() (cmd Command) {
	cmd.Name = cmds.EXIST
	cmd.Params = builder.params
	return
}

func Expire(key string) ExpireBuilder {
	return ExpireBuilder{
		params: []string{key},
	}
}

type ExpireBuilder struct {
	params []string
}

func (builder ExpireBuilder) Seconds(sec int64) ExpireBuilder {
	builder.params = append(builder.params, strconv.FormatInt(sec, 10))
	return builder
}

func (builder ExpireBuilder) Build() (cmd Command) {
	cmd.Name = cmds.EXPIRE
	cmd.Params = builder.params
	return
}

func ExpireAT(key string) ExpireAtBuilder {
	return ExpireAtBuilder{
		params: []string{key},
	}
}

type ExpireAtBuilder struct {
	params []string
}

func (builder ExpireAtBuilder) AT(t time.Time) ExpireAtBuilder {
	builder.params = append(builder.params, strconv.FormatInt(t.UnixMilli()/1000, 10))
	return builder
}

func (builder ExpireAtBuilder) Build() (cmd Command) {
	cmd.Name = cmds.EXPIREAT
	cmd.Params = builder.params
	return
}

func TTL(key string) TTLBuilder {
	return TTLBuilder{
		params: []string{key},
	}
}

type TTLBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder TTLBuilder) CacheTTL(ttl time.Duration) TTLBuilder {
	builder.ttl = ttl
	return builder
}

func (builder TTLBuilder) Build() (cmd Command) {
	cmd.Name = cmds.TTL
	cmd.Params = builder.params
	cmd.TTL = builder.ttl
	return
}

func PExpire(key string) PExpireBuilder {
	return PExpireBuilder{
		params: []string{key},
	}
}

type PExpireBuilder struct {
	params []string
}

func (builder PExpireBuilder) Milliseconds(n int64) PExpireBuilder {
	builder.params = append(builder.params, strconv.FormatInt(n, 10))
	return builder
}

func (builder PExpireBuilder) Build() (cmd Command) {
	cmd.Name = cmds.PEXPIRE
	cmd.Params = builder.params
	return
}

func PExpireAT(key string) PExpireAtBuilder {
	return PExpireAtBuilder{
		params: []string{key},
	}
}

type PExpireAtBuilder struct {
	params []string
}

func (builder PExpireAtBuilder) AT(t time.Time) PExpireAtBuilder {
	builder.params = append(builder.params, strconv.FormatInt(t.UnixMilli(), 10))
	return builder
}

func (builder PExpireAtBuilder) Build() (cmd Command) {
	cmd.Name = cmds.PEXPIREAT
	cmd.Params = builder.params
	return
}

func PTTL(key string) PTTLBuilder {
	return PTTLBuilder{
		params: []string{key},
	}
}

type PTTLBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder PTTLBuilder) CacheTTL(ttl time.Duration) PTTLBuilder {
	builder.ttl = ttl
	return builder
}

func (builder PTTLBuilder) Build() (cmd Command) {
	cmd.Name = cmds.PTTL
	cmd.Params = builder.params
	return
}

func Persist(key string) PersistBuilder {
	return PersistBuilder{
		params: []string{key},
	}
}

type PersistBuilder struct {
	params []string
}

func (builder PersistBuilder) Build() (cmd Command) {
	cmd.Name = cmds.PERSIST
	cmd.Params = builder.params
	return
}

func Rename(key string, newKey string) RenameBuilder {
	return RenameBuilder{
		params: []string{key, newKey},
	}
}

type RenameBuilder struct {
	params []string
}

func (builder RenameBuilder) Build() (cmd Command) {
	cmd.Name = cmds.RENAME
	cmd.Params = builder.params
	return
}

func RenameNX(key string, newKey string) RenameNxBuilder {
	return RenameNxBuilder{
		params: []string{key, newKey},
	}
}

type RenameNxBuilder struct {
	params []string
}

func (builder RenameNxBuilder) Build() (cmd Command) {
	cmd.Name = cmds.RENAMENX
	cmd.Params = builder.params
	return
}

func Scan(key string, cursor int64) ScanBuilder {
	return ScanBuilder{
		params: []string{key, strconv.FormatInt(cursor, 10)},
	}
}

type ScanBuilder struct {
	params []string
}

func (builder ScanBuilder) Match(pattern string) ScanBuilder {
	builder.params = append(builder.params, fmt.Sprintf("MATCH:%s", pattern))
	return builder
}

func (builder ScanBuilder) Count(n int64) ScanBuilder {
	builder.params = append(builder.params, fmt.Sprintf("COUNT:%d", n))
	return builder
}

func (builder ScanBuilder) Type(typ string) ScanBuilder {
	builder.params = append(builder.params, fmt.Sprintf("TYPE:%s", typ))
	return builder
}

func (builder ScanBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SCAN
	cmd.Params = builder.params
	return
}

func Sort(key string) SortBuilder {
	return SortBuilder{
		params: []string{key},
	}
}

type SortBuilder struct {
	params []string
}

func (builder SortBuilder) By(pattern string) SortBuilder {
	builder.params = append(builder.params, fmt.Sprintf("BY:%s", pattern))
	return builder
}

func (builder SortBuilder) Limit(offset int64, limit int64) SortBuilder {
	builder.params = append(builder.params, fmt.Sprintf("LIMIT:%d,%d", offset, limit))
	return builder
}

func (builder SortBuilder) Get(pattern string) SortBuilder {
	builder.params = append(builder.params, fmt.Sprintf("GET:%s", pattern))
	return builder
}

func (builder SortBuilder) Asc() SortBuilder {
	builder.params = append(builder.params, fmt.Sprintf("ORDER:%s", "asc"))
	return builder
}

func (builder SortBuilder) Desc() SortBuilder {
	builder.params = append(builder.params, fmt.Sprintf("ORDER:%s", "desc"))
	return builder
}

func (builder SortBuilder) Alpha() SortBuilder {
	builder.params = append(builder.params, fmt.Sprintf("%s", "ALPHA"))
	return builder
}

func (builder SortBuilder) Store(dst string) SortBuilder {
	builder.params = append(builder.params, fmt.Sprintf("STORE:%s", dst))
	return builder
}

func (builder SortBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SORT
	cmd.Params = builder.params
	return
}

func SortRO(key string) SortRoBuilder {
	return SortRoBuilder{
		params: []string{key},
	}
}

type SortRoBuilder struct {
	params []string
}

func (builder SortRoBuilder) By(pattern string) SortRoBuilder {
	builder.params = append(builder.params, fmt.Sprintf("BY:%s", pattern))
	return builder
}

func (builder SortRoBuilder) Limit(offset int64, limit int64) SortRoBuilder {
	builder.params = append(builder.params, fmt.Sprintf("LIMIT:%d,%d", offset, limit))
	return builder
}

func (builder SortRoBuilder) Get(pattern string) SortRoBuilder {
	builder.params = append(builder.params, fmt.Sprintf("GET:%s", pattern))
	return builder
}

func (builder SortRoBuilder) Asc() SortRoBuilder {
	builder.params = append(builder.params, fmt.Sprintf("ORDER:%s", "asc"))
	return builder
}

func (builder SortRoBuilder) Desc() SortRoBuilder {
	builder.params = append(builder.params, fmt.Sprintf("ORDER:%s", "desc"))
	return builder
}

func (builder SortRoBuilder) Alpha() SortRoBuilder {
	builder.params = append(builder.params, fmt.Sprintf("%s", "ALPHA"))
	return builder
}

func (builder SortRoBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SORTRO
	cmd.Params = builder.params
	return
}

func Touch(key string) TouchBuilder {
	return TouchBuilder{
		params: []string{key},
	}
}

type TouchBuilder struct {
	params []string
}

func (builder TouchBuilder) Build() (cmd Command) {
	cmd.Name = cmds.TOUCH
	cmd.Params = builder.params
	return
}

func Unlinks(key ...string) UnlinksBuilder {
	return UnlinksBuilder{
		params: key,
	}
}

type UnlinksBuilder struct {
	params []string
}

func (builder UnlinksBuilder) Build() (cmd Command) {
	cmd.Name = cmds.UNLINKS
	cmd.Params = builder.params
	return
}
