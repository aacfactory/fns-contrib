package redis

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/redis/cmds"
	"strconv"
	"time"
)

func Append(key string, value string) AppendBuilder {
	return AppendBuilder{
		params: []string{key, value},
	}
}

type AppendBuilder struct {
	params []string
}

func (builder AppendBuilder) Build() (cmd Command) {
	cmd.Name = cmds.APPEND
	cmd.Params = builder.params
	return
}

func Decr(key string) DecrBuilder {
	return DecrBuilder{
		params: []string{key},
	}
}

type DecrBuilder struct {
	params []string
}

func (builder DecrBuilder) Build() (cmd Command) {
	cmd.Name = cmds.DECR
	cmd.Params = builder.params
	return
}

func DecrBy(key string, delta int64) DecrByBuilder {
	return DecrByBuilder{
		params: []string{key, strconv.FormatInt(delta, 10)},
	}
}

type DecrByBuilder struct {
	params []string
}

func (builder DecrByBuilder) Build() (cmd Command) {
	cmd.Name = cmds.DECRBY
	cmd.Params = builder.params
	return
}

func Get(key string) GetBuilder {
	return GetBuilder{
		params: []string{key},
	}
}

type GetBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder GetBuilder) CacheTTL(ttl time.Duration) GetBuilder {
	builder.ttl = ttl
	return builder
}

func (builder GetBuilder) Build() (cmd Command) {
	cmd.Name = cmds.GET
	cmd.Params = builder.params
	return
}

func GetDel(key string) GetDelBuilder {
	return GetDelBuilder{
		params: []string{key},
	}
}

type GetDelBuilder struct {
	params []string
}

func (builder GetDelBuilder) Build() (cmd Command) {
	cmd.Name = cmds.GETDEL
	cmd.Params = builder.params
	return
}

func GetEX(key string) GetExBuilder {
	return GetExBuilder{
		params: []string{key},
	}
}

type GetExBuilder struct {
	params []string
}

func (builder GetExBuilder) Persist() GetExBuilder {
	builder.params = append(builder.params, "Persist")
	return builder
}

func (builder GetExBuilder) Ex(v time.Duration) GetExBuilder {
	builder.params = append(builder.params, fmt.Sprintf("EX:%s", v.String()))
	return builder
}

func (builder GetExBuilder) ExAT(v time.Time) GetExBuilder {
	builder.params = append(builder.params, fmt.Sprintf("EXAT:%s", v.Format(time.RFC3339)))
	return builder
}

func (builder GetExBuilder) Px(v time.Duration) GetExBuilder {
	builder.params = append(builder.params, fmt.Sprintf("PX:%s", v.String()))
	return builder
}

func (builder GetExBuilder) PxAT(v time.Time) GetExBuilder {
	builder.params = append(builder.params, fmt.Sprintf("PXAT:%s", v.Format(time.RFC3339)))
	return builder
}

func (builder GetExBuilder) Build() (cmd Command) {
	cmd.Name = cmds.GETEX
	cmd.Params = builder.params
	return
}

func GetRange(key string, start int64, end int64) GetRangeBuilder {
	return GetRangeBuilder{
		params: []string{key, strconv.FormatInt(start, 10), strconv.FormatInt(end, 10)},
	}
}

type GetRangeBuilder struct {
	params []string
}

func (builder GetRangeBuilder) Build() (cmd Command) {
	cmd.Name = cmds.GETRANGE
	cmd.Params = builder.params
	return
}

func GetSet(key string, value string) GetSetBuilder {
	return GetSetBuilder{
		params: []string{key, value},
	}
}

type GetSetBuilder struct {
	params []string
}

func (builder GetSetBuilder) Build() (cmd Command) {
	cmd.Name = cmds.GETSET
	cmd.Params = builder.params
	return
}

func Incr(key string) IncrBuilder {
	return IncrBuilder{
		params: []string{key},
	}
}

type IncrBuilder struct {
	params []string
}

func (builder IncrBuilder) Build() (cmd Command) {
	cmd.Name = cmds.INCR
	cmd.Params = builder.params
	return
}

func IncrBy(key string, delta int64) IncrByBuilder {
	return IncrByBuilder{
		params: []string{key, strconv.FormatInt(delta, 10)},
	}
}

type IncrByBuilder struct {
	params []string
}

func (builder IncrByBuilder) Build() (cmd Command) {
	cmd.Name = cmds.INCRBY
	cmd.Params = builder.params
	return
}

func IncrByFloat(key string, delta float64) IncrByFloatBuilder {
	return IncrByFloatBuilder{
		params: []string{key, strconv.FormatFloat(delta, 'f', 6, 10)},
	}
}

type IncrByFloatBuilder struct {
	params []string
}

func (builder IncrByFloatBuilder) Build() (cmd Command) {
	cmd.Name = cmds.INCRBYFLOAT
	cmd.Params = builder.params
	return
}

func LCS(key1 string, key2 string) LCSBuilder {
	return LCSBuilder{
		params: []string{key1, key2},
	}
}

type LCSBuilder struct {
	params []string
}

func (builder LCSBuilder) Len() LCSBuilder {
	builder.params = append(builder.params, "LEN")
	return builder
}

func (builder LCSBuilder) Idx() LCSBuilder {
	builder.params = append(builder.params, "IDX")
	return builder
}

func (builder LCSBuilder) MinMatchLen(n int64) LCSBuilder {
	builder.params = append(builder.params, fmt.Sprintf("MINMATCHLEN:%d", n))
	return builder
}

func (builder LCSBuilder) WithMatchLen() LCSBuilder {
	builder.params = append(builder.params, "WITHMATCHLEN")
	return builder
}

func (builder LCSBuilder) Build() (cmd Command) {
	cmd.Name = cmds.LCS
	cmd.Params = builder.params
	return
}

func MGet(key ...string) MGetBuilder {
	return MGetBuilder{
		params: key,
	}
}

type MGetBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder MGetBuilder) CacheTTL(ttl time.Duration) MGetBuilder {
	builder.ttl = ttl
	return builder
}

func (builder MGetBuilder) Build() (cmd Command) {
	cmd.Name = cmds.MGET
	cmd.Params = builder.params
	return
}

func MSet() MGetBuilder {
	return MGetBuilder{
		params: make([]string, 0, 2),
	}
}

type MSetBuilder struct {
	params []string
}

func (builder MSetBuilder) KeyValue(key string, value string) MSetBuilder {
	builder.params = append(builder.params, key, value)
	return builder
}

func (builder MSetBuilder) Build() (cmd Command) {
	cmd.Name = cmds.MSET
	cmd.Params = builder.params
	return
}

func MSetNX() MSetNxBuilder {
	return MSetNxBuilder{
		params: make([]string, 0, 2),
	}
}

type MSetNxBuilder struct {
	params []string
}

func (builder MSetNxBuilder) KeyValue(key string, value string) MSetNxBuilder {
	builder.params = append(builder.params, key, value)
	return builder
}

func (builder MSetNxBuilder) Build() (cmd Command) {
	cmd.Name = cmds.MSETNX
	cmd.Params = builder.params
	return
}

func Set(key string, value string) SetBuilder {
	return SetBuilder{
		params: []string{key, value},
	}
}

type SetBuilder struct {
	params []string
}

func (builder SetBuilder) NX() SetBuilder {
	builder.params = append(builder.params, "NX")
	return builder
}

func (builder SetBuilder) XX() SetBuilder {
	builder.params = append(builder.params, "XX")
	return builder
}

func (builder SetBuilder) Get() SetBuilder {
	builder.params = append(builder.params, "GET")
	return builder
}

func (builder SetBuilder) Ex(v time.Duration) SetBuilder {
	builder.params = append(builder.params, fmt.Sprintf("EX:%s", v.String()))
	return builder
}

func (builder SetBuilder) Px(v time.Duration) SetBuilder {
	builder.params = append(builder.params, fmt.Sprintf("PX:%s", v.String()))
	return builder
}

func (builder SetBuilder) ExAT(v time.Time) SetBuilder {
	builder.params = append(builder.params, fmt.Sprintf("EXAT:%s", v.Format(time.RFC3339)))
	return builder
}

func (builder SetBuilder) PxAT(v time.Time) SetBuilder {
	builder.params = append(builder.params, fmt.Sprintf("PXAT:%s", v.Format(time.RFC3339)))
	return builder
}

func (builder SetBuilder) KeepTTL() SetBuilder {
	builder.params = append(builder.params, "KEEPTTL")
	return builder
}

func (builder SetBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SET
	cmd.Params = builder.params
	return
}

func SetRange(key string, offset int64, value string) SetRangeBuilder {
	return SetRangeBuilder{
		params: []string{key, strconv.FormatInt(offset, 10), value},
	}
}

type SetRangeBuilder struct {
	params []string
}

func (builder SetRangeBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SETRANGE
	cmd.Params = builder.params
	return
}

func StrLen(key string) StrLenBuilder {
	return StrLenBuilder{
		params: []string{key},
	}
}

type StrLenBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder StrLenBuilder) CacheTTL(ttl time.Duration) StrLenBuilder {
	builder.ttl = ttl
	return builder
}

func (builder StrLenBuilder) Build() (cmd Command) {
	cmd.Name = cmds.STRLEN
	cmd.Params = builder.params
	return
}
