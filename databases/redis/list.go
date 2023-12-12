package redis

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/redis/cmds"
	"strconv"
	"time"
)

func BLMove(src string, dst string) BLMOVEBuilder {
	return BLMOVEBuilder{
		params: []string{src, dst},
	}
}

type BLMOVEBuilder struct {
	params []string
}

func (builder BLMOVEBuilder) Left() BLMOVEBuilder {
	builder.params = append(builder.params, "LEFT")
	return builder
}

func (builder BLMOVEBuilder) Right() BLMOVEBuilder {
	builder.params = append(builder.params, "RIGHT")
	return builder
}

func (builder BLMOVEBuilder) Timeout(timeout float64) BLMOVEBuilder {
	builder.params = append(builder.params, fmt.Sprintf("TIMEOUT:%v", timeout))
	return builder
}

func (builder BLMOVEBuilder) Build() (cmd Command) {
	cmd.Name = cmds.BLMOVE
	cmd.Params = builder.params
	return
}

func BLMPop(timeout float64, numKeys int64) BLMPOPBuilder {
	return BLMPOPBuilder{
		params: []string{strconv.FormatFloat(timeout, 'f', 6, 64), strconv.FormatInt(numKeys, 10)},
	}
}

type BLMPOPBuilder struct {
	params []string
}

func (builder BLMPOPBuilder) Key(key ...string) BLMPOPBuilder {
	for _, s := range key {
		builder.params = append(builder.params, fmt.Sprintf("KEY:%s", s))
	}
	return builder
}

func (builder BLMPOPBuilder) Left() BLMPOPBuilder {
	builder.params = append(builder.params, "LEFT")
	return builder
}

func (builder BLMPOPBuilder) Right() BLMPOPBuilder {
	builder.params = append(builder.params, "RIGHT")
	return builder
}

func (builder BLMPOPBuilder) Count(count int64) BLMPOPBuilder {
	builder.params = append(builder.params, fmt.Sprintf("COUNT:%d", count))
	return builder
}

func (builder BLMPOPBuilder) Build() (cmd Command) {
	cmd.Name = cmds.BLMPOP
	cmd.Params = builder.params
	return
}

func BLPop(key ...string) BLPOPBuilder {
	params := make([]string, 0, len(key))
	for _, s := range key {
		params = append(params, fmt.Sprintf("KEY:%s", s))
	}
	return BLPOPBuilder{
		params: params,
	}
}

type BLPOPBuilder struct {
	params []string
}

func (builder BLPOPBuilder) Timeout(timeout float64) BLPOPBuilder {
	builder.params = append(builder.params, fmt.Sprintf("TIMEOUT:%v", timeout))
	return builder
}

func (builder BLPOPBuilder) Build() (cmd Command) {
	cmd.Name = cmds.BLPOP
	cmd.Params = builder.params
	return
}

func BRPop(key ...string) BRPOPBuilder {
	params := make([]string, 0, len(key))
	for _, s := range key {
		params = append(params, fmt.Sprintf("KEY:%s", s))
	}
	return BRPOPBuilder{
		params: params,
	}
}

type BRPOPBuilder struct {
	params []string
}

func (builder BRPOPBuilder) Timeout(timeout float64) BRPOPBuilder {
	builder.params = append(builder.params, fmt.Sprintf("TIMEOUT:%v", timeout))
	return builder
}

func (builder BRPOPBuilder) Build() (cmd Command) {
	cmd.Name = cmds.BRPOP
	cmd.Params = builder.params
	return
}

func LIndex(key string, index int64) LINDEXBuilder {
	return LINDEXBuilder{
		params: []string{key, strconv.FormatInt(index, 10)},
	}
}

type LINDEXBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder LINDEXBuilder) CacheTTL(ttl time.Duration) LINDEXBuilder {
	builder.ttl = ttl
	return builder
}

func (builder LINDEXBuilder) Build() (cmd Command) {
	cmd.Name = cmds.LINDEX
	cmd.Params = builder.params
	return
}

func LInsert(key string) LINSERTBuilder {
	return LINSERTBuilder{
		params: []string{key},
	}
}

type LINSERTBuilder struct {
	params []string
}

func (builder LINSERTBuilder) Before() LINSERTBuilder {
	builder.params = append(builder.params, "BEFORE")
	return builder
}

func (builder LINSERTBuilder) After() LINSERTBuilder {
	builder.params = append(builder.params, "AFTER")
	return builder
}

func (builder LINSERTBuilder) Pivot(pivot string) LINSERTBuilder {
	builder.params = append(builder.params, pivot)
	return builder
}

func (builder LINSERTBuilder) Element(element string) LINSERTBuilder {
	builder.params = append(builder.params, element)
	return builder
}

func (builder LINSERTBuilder) Build() (cmd Command) {
	cmd.Name = cmds.LINSERT
	cmd.Params = builder.params
	return
}

func LLen(key string) LLENBuilder {
	return LLENBuilder{
		params: []string{key},
	}
}

type LLENBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder LLENBuilder) CacheTTL(ttl time.Duration) LLENBuilder {
	builder.ttl = ttl
	return builder
}

func (builder LLENBuilder) Build() (cmd Command) {
	cmd.Name = cmds.LLEN
	cmd.Params = builder.params
	return
}

func LMove(src string, dst string) LMOVEBuilder {
	return LMOVEBuilder{
		params: []string{src, dst},
	}
}

type LMOVEBuilder struct {
	params []string
}

func (builder LMOVEBuilder) Left() LMOVEBuilder {
	builder.params = append(builder.params, "LEFT")
	return builder
}

func (builder LMOVEBuilder) Right() LMOVEBuilder {
	builder.params = append(builder.params, "RIGHT")
	return builder
}

func (builder LMOVEBuilder) Build() (cmd Command) {
	cmd.Name = cmds.LMOVE
	cmd.Params = builder.params
	return
}

func LMPop(numKey int64) LMPOPBuilder {
	return LMPOPBuilder{
		params: []string{strconv.FormatInt(numKey, 10)},
	}
}

type LMPOPBuilder struct {
	params []string
}

func (builder LMPOPBuilder) Key(key ...string) LMPOPBuilder {
	for _, s := range key {
		builder.params = append(builder.params, fmt.Sprintf("KEY:%s", s))
	}
	return builder
}

func (builder LMPOPBuilder) Left() LMPOPBuilder {
	builder.params = append(builder.params, "LEFT")
	return builder
}

func (builder LMPOPBuilder) Right() LMPOPBuilder {
	builder.params = append(builder.params, "RIGHT")
	return builder
}

func (builder LMPOPBuilder) Count(count int64) LMPOPBuilder {
	builder.params = append(builder.params, fmt.Sprintf("COUNT:%d", count))
	return builder
}

func (builder LMPOPBuilder) Build() (cmd Command) {
	cmd.Name = cmds.LMPOP
	cmd.Params = builder.params
	return
}

func LPop(key string) LPOPBuilder {
	return LPOPBuilder{
		params: []string{key},
	}
}

type LPOPBuilder struct {
	params []string
}

func (builder LPOPBuilder) Count(count int64) LPOPBuilder {
	builder.params = append(builder.params, fmt.Sprintf("COUNT:%d", count))
	return builder
}

func (builder LPOPBuilder) Build() (cmd Command) {
	cmd.Name = cmds.LPOP
	cmd.Params = builder.params
	return
}

func LPos(key string, element string) LPOSBuilder {
	return LPOSBuilder{
		params: []string{key, element},
	}
}

type LPOSBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder LPOSBuilder) Rank(rank int64) LPOSBuilder {
	builder.params = append(builder.params, fmt.Sprintf("RANK:%d", rank))
	return builder
}

func (builder LPOSBuilder) Count(count int64) LPOSBuilder {
	builder.params = append(builder.params, fmt.Sprintf("COUNT:%d", count))
	return builder
}

func (builder LPOSBuilder) MaxLen(maxLen int64) LPOSBuilder {
	builder.params = append(builder.params, fmt.Sprintf("MAXLEN:%d", maxLen))
	return builder
}

func (builder LPOSBuilder) CacheTTL(ttl time.Duration) LPOSBuilder {
	builder.ttl = ttl
	return builder
}

func (builder LPOSBuilder) Build() (cmd Command) {
	cmd.Name = cmds.LPOS
	cmd.Params = builder.params
	return
}

func LPush(key string, element ...string) LPUSHBuilder {
	return LPUSHBuilder{
		params: append([]string{key}, element...),
	}
}

type LPUSHBuilder struct {
	params []string
}

func (builder LPUSHBuilder) Build() (cmd Command) {
	cmd.Name = cmds.LPUSH
	cmd.Params = builder.params
	return
}

func LPushX(key string, element ...string) LPUSHXBuilder {
	return LPUSHXBuilder{
		params: append([]string{key}, element...),
	}
}

type LPUSHXBuilder struct {
	params []string
}

func (builder LPUSHXBuilder) Build() (cmd Command) {
	cmd.Name = cmds.LPUSHX
	cmd.Params = builder.params
	return
}

func LRange(key string, start int64, stop int64) LRANGEBuilder {
	return LRANGEBuilder{
		params: []string{key, strconv.FormatInt(start, 10), strconv.FormatInt(stop, 10)},
	}
}

type LRANGEBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder LRANGEBuilder) CacheTTL(ttl time.Duration) LRANGEBuilder {
	builder.ttl = ttl
	return builder
}

func (builder LRANGEBuilder) Build() (cmd Command) {
	cmd.Name = cmds.LRANGE
	cmd.Params = builder.params
	return
}

func LRem(key string, count int64, element string) LREMBuilder {
	return LREMBuilder{
		params: []string{key, strconv.FormatInt(count, 10), element},
	}
}

type LREMBuilder struct {
	params []string
}

func (builder LREMBuilder) Build() (cmd Command) {
	cmd.Name = cmds.LREM
	cmd.Params = builder.params
	return
}

func LSet(key string, index int64, element string) LSETBuilder {
	return LSETBuilder{
		params: []string{key, strconv.FormatInt(index, 10), element},
	}
}

type LSETBuilder struct {
	params []string
}

func (builder LSETBuilder) Build() (cmd Command) {
	cmd.Name = cmds.LSET
	cmd.Params = builder.params
	return
}

func LTRIM(key string, start int64, stop int64) LTRIMBuilder {
	return LTRIMBuilder{
		params: []string{key, strconv.FormatInt(start, 10), strconv.FormatInt(stop, 10)},
	}
}

type LTRIMBuilder struct {
	params []string
}

func (builder LTRIMBuilder) Build() (cmd Command) {
	cmd.Name = cmds.LTRIM
	cmd.Params = builder.params
	return
}

func RPop(key string) RPOPBuilder {
	return RPOPBuilder{
		params: []string{key},
	}
}

type RPOPBuilder struct {
	params []string
}

func (builder RPOPBuilder) Count(count int64) RPOPBuilder {
	builder.params = append(builder.params, fmt.Sprintf("COUNT:%d", count))
	return builder
}

func (builder RPOPBuilder) Build() (cmd Command) {
	cmd.Name = cmds.RPOP
	cmd.Params = builder.params
	return
}

func RPush(key string, element ...string) RPUSHBuilder {
	return RPUSHBuilder{
		params: append([]string{key}, element...),
	}
}

type RPUSHBuilder struct {
	params []string
}

func (builder RPUSHBuilder) Build() (cmd Command) {
	cmd.Name = cmds.RPUSH
	cmd.Params = builder.params
	return
}

func RPushX(key string, element ...string) RPUSHXBuilder {
	return RPUSHXBuilder{
		params: append([]string{key}, element...),
	}
}

type RPUSHXBuilder struct {
	params []string
}

func (builder RPUSHXBuilder) Build() (cmd Command) {
	cmd.Name = cmds.RPUSHX
	cmd.Params = builder.params
	return
}
