package redis

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/redis/cmds"
	"strconv"
	"time"
)

func HDel(key string, fields ...string) HDELBuilder {
	return HDELBuilder{
		params: append([]string{key}, fields...),
	}
}

type HDELBuilder struct {
	params []string
}

func (builder HDELBuilder) Build() (cmd Command) {
	cmd.Name = cmds.HDEL
	cmd.Params = builder.params
	return
}

func HExist(key string, field string) HEXISTSBuilder {
	return HEXISTSBuilder{
		params: []string{key, field},
	}
}

type HEXISTSBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder HEXISTSBuilder) CacheTTL(ttl time.Duration) HEXISTSBuilder {
	builder.ttl = ttl
	return builder
}

func (builder HEXISTSBuilder) Build() (cmd Command) {
	cmd.Name = cmds.HEXISTS
	cmd.Params = builder.params
	return
}

func HGet(key string, field string) HGETBuilder {
	return HGETBuilder{
		params: []string{key, field},
	}
}

type HGETBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder HGETBuilder) CacheTTL(ttl time.Duration) HGETBuilder {
	builder.ttl = ttl
	return builder
}

func (builder HGETBuilder) Build() (cmd Command) {
	cmd.Name = cmds.HGET
	cmd.Params = builder.params
	return
}

func HGetALL(key string) HGETALLBuilder {
	return HGETALLBuilder{
		params: []string{key},
	}
}

type HGETALLBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder HGETALLBuilder) CacheTTL(ttl time.Duration) HGETALLBuilder {
	builder.ttl = ttl
	return builder
}

func (builder HGETALLBuilder) Build() (cmd Command) {
	cmd.Name = cmds.HGETALL
	cmd.Params = builder.params
	return
}

func HIncrBy(key string, field string, delta int64) HINCRBYBuilder {
	return HINCRBYBuilder{
		params: []string{key, field, strconv.FormatInt(delta, 10)},
	}
}

type HINCRBYBuilder struct {
	params []string
}

func (builder HINCRBYBuilder) Build() (cmd Command) {
	cmd.Name = cmds.HINCRBY
	cmd.Params = builder.params
	return
}

func HIncrByFloat(key string, field string, delta float64) HINCRBYBuilder {
	return HINCRBYBuilder{
		params: []string{key, field, strconv.FormatFloat(delta, 'f', 6, 64)},
	}
}

type HINCRBYFLOATBuilder struct {
	params []string
}

func (builder HINCRBYFLOATBuilder) Build() (cmd Command) {
	cmd.Name = cmds.HINCRBYFLOAT
	cmd.Params = builder.params
	return
}

func HKeys(key string) HKEYSBuilder {
	return HKEYSBuilder{
		params: []string{key},
	}
}

type HKEYSBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder HKEYSBuilder) CacheTTL(ttl time.Duration) HKEYSBuilder {
	builder.ttl = ttl
	return builder
}

func (builder HKEYSBuilder) Build() (cmd Command) {
	cmd.Name = cmds.HKEYS
	cmd.Params = builder.params
	return
}

func HLen(key string) HLENBuilder {
	return HLENBuilder{
		params: []string{key},
	}
}

type HLENBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder HLENBuilder) CacheTTL(ttl time.Duration) HLENBuilder {
	builder.ttl = ttl
	return builder
}

func (builder HLENBuilder) Build() (cmd Command) {
	cmd.Name = cmds.HLEN
	cmd.Params = builder.params
	return
}

func HMGet(key string, fields ...string) HMGETBuilder {
	return HMGETBuilder{
		params: append([]string{key}, fields...),
	}
}

type HMGETBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder HMGETBuilder) CacheTTL(ttl time.Duration) HMGETBuilder {
	builder.ttl = ttl
	return builder
}

func (builder HMGETBuilder) Build() (cmd Command) {
	cmd.Name = cmds.HMGET
	cmd.Params = builder.params
	return
}

func HRandField(key string) HRANDFIELDBuilder {
	return HRANDFIELDBuilder{
		params: []string{key},
	}
}

type HRANDFIELDBuilder struct {
	params []string
}

func (builder HRANDFIELDBuilder) Count(n int64) HRANDFIELDBuilder {
	builder.params = append(builder.params, fmt.Sprintf("COUNT:%d", n))
	return builder
}

func (builder HRANDFIELDBuilder) WithValues() HRANDFIELDBuilder {
	builder.params = append(builder.params, "WITHVALUES")
	return builder
}

func (builder HRANDFIELDBuilder) Build() (cmd Command) {
	cmd.Name = cmds.HRANDFIELD
	cmd.Params = builder.params
	return
}

func HScan(key string, cursor uint64) HSCANBuilder {
	return HSCANBuilder{
		params: []string{key, strconv.FormatUint(cursor, 10)},
	}
}

func (builder HSCANBuilder) Match(pattern string) HSCANBuilder {
	builder.params = append(builder.params, fmt.Sprintf("MATCH:%s", pattern))
	return builder
}

func (builder HSCANBuilder) Count(n int64) HSCANBuilder {
	builder.params = append(builder.params, fmt.Sprintf("COUNT:%d", n))
	return builder
}

type HSCANBuilder struct {
	params []string
}

func (builder HSCANBuilder) Build() (cmd Command) {
	cmd.Name = cmds.HSCAN
	cmd.Params = builder.params
	return
}

func HSet(key string) HSETBuilder {
	return HSETBuilder{
		params: []string{key},
	}
}

type HSETBuilder struct {
	params []string
}

func (builder HSETBuilder) KeyValue(key string, value string) HSETBuilder {
	builder.params = append(builder.params, key, value)
	return builder
}

func (builder HSETBuilder) Build() (cmd Command) {
	cmd.Name = cmds.HSET
	cmd.Params = builder.params
	return
}

func HSetNX(key string) HSETNXBuilder {
	return HSETNXBuilder{
		params: []string{key},
	}
}

type HSETNXBuilder struct {
	params []string
}

func (builder HSETNXBuilder) KeyValue(key string, value string) HSETNXBuilder {
	builder.params = append(builder.params, key, value)
	return builder
}

func (builder HSETNXBuilder) Build() (cmd Command) {
	cmd.Name = cmds.HSETNX
	cmd.Params = builder.params
	return
}

func HStrLen(key string, field string) HSTRLENBuilder {
	return HSTRLENBuilder{
		params: []string{key, field},
	}
}

type HSTRLENBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder HSTRLENBuilder) CacheTTL(ttl time.Duration) HSTRLENBuilder {
	builder.ttl = ttl
	return builder
}

func (builder HSTRLENBuilder) Build() (cmd Command) {
	cmd.Name = cmds.HSTRLEN
	cmd.Params = builder.params
	return
}

func HValues(key string) HVALSBuilder {
	return HVALSBuilder{
		params: []string{key},
	}
}

type HVALSBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder HVALSBuilder) CacheTTL(ttl time.Duration) HVALSBuilder {
	builder.ttl = ttl
	return builder
}

func (builder HVALSBuilder) Build() (cmd Command) {
	cmd.Name = cmds.HVALS
	cmd.Params = builder.params
	return
}
