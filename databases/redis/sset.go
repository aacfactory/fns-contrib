package redis

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/redis/cmds"
	"strconv"
	"time"
)

func BZMPop(timeout float64, numKeys int64) BZMPOPBuilder {
	return BZMPOPBuilder{
		params: []string{strconv.FormatFloat(timeout, 'f', 6, 64), strconv.FormatInt(numKeys, 10)},
	}
}

type BZMPOPBuilder struct {
	params []string
}

func (builder BZMPOPBuilder) Key(keys ...string) BZMPOPBuilder {
	for _, key := range keys {
		builder.params = append(builder.params, fmt.Sprintf("KEY:%s", key))
	}
	return builder
}

func (builder BZMPOPBuilder) Min() BZMPOPBuilder {
	builder.params = append(builder.params, "MIN")
	return builder
}

func (builder BZMPOPBuilder) Max() BZMPOPBuilder {
	builder.params = append(builder.params, "MAX")
	return builder
}

func (builder BZMPOPBuilder) Count(count int64) BZMPOPBuilder {
	builder.params = append(builder.params, fmt.Sprintf("COUNT:%d", count))
	return builder
}

func (builder BZMPOPBuilder) Build() (cmd Command) {
	cmd.Name = cmds.BZMPOP
	cmd.Params = builder.params
	return
}

func BZPopMax(timeout float64, keys ...string) BZPOPMAXBuilder {
	return BZPOPMAXBuilder{
		params: append([]string{strconv.FormatFloat(timeout, 'f', 6, 64)}, keys...),
	}
}

type BZPOPMAXBuilder struct {
	params []string
}

func (builder BZPOPMAXBuilder) Build() (cmd Command) {
	cmd.Name = cmds.BZPOPMAX
	cmd.Params = builder.params
	return
}

func ZAdd(key string) ZADDBuilder {
	return ZADDBuilder{
		params: []string{key},
	}
}

type ZADDBuilder struct {
	params []string
}

func (builder ZADDBuilder) Nx() ZADDBuilder {
	builder.params = append(builder.params, "NX")
	return builder
}

func (builder ZADDBuilder) Xx() ZADDBuilder {
	builder.params = append(builder.params, "XX")
	return builder
}

func (builder ZADDBuilder) Gt() ZADDBuilder {
	builder.params = append(builder.params, "GT")
	return builder
}

func (builder ZADDBuilder) Lt() ZADDBuilder {
	builder.params = append(builder.params, "LT")
	return builder
}

func (builder ZADDBuilder) Ch() ZADDBuilder {
	builder.params = append(builder.params, "CH")
	return builder
}

func (builder ZADDBuilder) Incr() ZADDBuilder {
	builder.params = append(builder.params, "INCR")
	return builder
}

func (builder ZADDBuilder) ScoreMember(score float64, member string) ZADDBuilder {
	builder.params = append(builder.params, fmt.Sprintf("SCORE:%v+%s", score, member))
	return builder
}

func (builder ZADDBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZADD
	cmd.Params = builder.params
	return
}

func ZCard(key string) ZCARDBuilder {
	return ZCARDBuilder{
		params: []string{key},
	}
}

type ZCARDBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder ZCARDBuilder) CacheTTL(ttl time.Duration) ZCARDBuilder {
	builder.ttl = ttl
	return builder
}

func (builder ZCARDBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZCARD
	cmd.Params = builder.params
	return
}

func ZCount(key string, min string, max string) ZCOUNTBuilder {
	return ZCOUNTBuilder{
		params: []string{key, min, max},
	}
}

type ZCOUNTBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder ZCOUNTBuilder) CacheTTL(ttl time.Duration) ZCOUNTBuilder {
	builder.ttl = ttl
	return builder
}

func (builder ZCOUNTBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZCOUNT
	cmd.Params = builder.params
	return
}

func ZDiff(numKey int64) ZDIFFBuilder {
	return ZDIFFBuilder{
		params: []string{fmt.Sprintf("%d", numKey)},
	}
}

type ZDIFFBuilder struct {
	params []string
}

func (builder ZDIFFBuilder) Key(keys ...string) ZDIFFBuilder {
	for _, key := range keys {
		builder.params = append(builder.params, fmt.Sprintf("KEY:%s", key))
	}
	return builder
}

func (builder ZDIFFBuilder) Withscores() ZDIFFBuilder {
	builder.params = append(builder.params, "WITHSCORES")
	return builder
}

func (builder ZDIFFBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZDIFF
	cmd.Params = builder.params
	return
}

func ZDiffStore(dst string, numKey int64, keys ...string) ZDIFFSTOREBuilder {
	return ZDIFFSTOREBuilder{
		params: append([]string{dst, fmt.Sprintf("%d", numKey)}, keys...),
	}
}

type ZDIFFSTOREBuilder struct {
	params []string
}

func (builder ZDIFFSTOREBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZDIFFSTORE
	cmd.Params = builder.params
	return
}

func ZInter(numKey int64) ZINTERBuilder {
	return ZINTERBuilder{
		params: []string{fmt.Sprintf("%d", numKey)},
	}
}

type ZINTERBuilder struct {
	params []string
}

func (builder ZINTERBuilder) Key(keys ...string) ZINTERBuilder {
	for _, key := range keys {
		builder.params = append(builder.params, fmt.Sprintf("KEY:%s", key))
	}
	return builder
}

func (builder ZINTERBuilder) Weights(weights ...int64) ZINTERBuilder {
	for _, weight := range weights {
		builder.params = append(builder.params, fmt.Sprintf("WEIGHT:%d", weight))
	}
	return builder
}

func (builder ZINTERBuilder) AggregateMax() ZINTERBuilder {
	builder.params = append(builder.params, "AGGMAX")
	return builder
}

func (builder ZINTERBuilder) AggregateMin() ZINTERBuilder {
	builder.params = append(builder.params, "AGGMIX")
	return builder
}

func (builder ZINTERBuilder) AggregateSum() ZINTERBuilder {
	builder.params = append(builder.params, "AGGSUM")
	return builder
}

func (builder ZINTERBuilder) Withscores() ZINTERBuilder {
	builder.params = append(builder.params, "WITHSCORES")
	return builder
}

func (builder ZINTERBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZINTER
	cmd.Params = builder.params
	return
}

func ZInterCard(numKey int64) ZINTERCARDBuilder {
	return ZINTERCARDBuilder{
		params: []string{fmt.Sprintf("%d", numKey)},
	}
}

type ZINTERCARDBuilder struct {
	params []string
}

func (builder ZINTERCARDBuilder) Key(keys ...string) ZINTERCARDBuilder {
	for _, key := range keys {
		builder.params = append(builder.params, fmt.Sprintf("KEY:%s", key))
	}
	return builder
}

func (builder ZINTERCARDBuilder) Limit(limit int64) ZINTERCARDBuilder {
	builder.params = append(builder.params, fmt.Sprintf("LIMIT:%d", limit))
	return builder
}

func (builder ZINTERCARDBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZINTERCARD
	cmd.Params = builder.params
	return
}

func ZInterStore(dst string, numKey int64) ZINTERSTOREBuilder {
	return ZINTERSTOREBuilder{
		params: []string{dst, fmt.Sprintf("%d", numKey)},
	}
}

type ZINTERSTOREBuilder struct {
	params []string
}

func (builder ZINTERSTOREBuilder) Key(keys ...string) ZINTERSTOREBuilder {
	for _, key := range keys {
		builder.params = append(builder.params, fmt.Sprintf("KEY:%s", key))
	}
	return builder
}

func (builder ZINTERSTOREBuilder) Weights(weights ...int64) ZINTERSTOREBuilder {
	for _, weight := range weights {
		builder.params = append(builder.params, fmt.Sprintf("WEIGHT:%d", weight))
	}
	return builder
}

func (builder ZINTERSTOREBuilder) AggregateMax() ZINTERSTOREBuilder {
	builder.params = append(builder.params, "AGGMAX")
	return builder
}

func (builder ZINTERSTOREBuilder) AggregateMin() ZINTERSTOREBuilder {
	builder.params = append(builder.params, "AGGMIX")
	return builder
}

func (builder ZINTERSTOREBuilder) AggregateSum() ZINTERSTOREBuilder {
	builder.params = append(builder.params, "AGGSUM")
	return builder
}

func (builder ZINTERSTOREBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZINTERSTORE
	cmd.Params = builder.params
	return
}

func ZLexCount(key string, min string, max string) ZLEXCOUNTBuilder {
	return ZLEXCOUNTBuilder{
		params: []string{key, min, max},
	}
}

type ZLEXCOUNTBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder ZLEXCOUNTBuilder) CacheTTL(ttl time.Duration) ZLEXCOUNTBuilder {
	builder.ttl = ttl
	return builder
}

func (builder ZLEXCOUNTBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZLEXCOUNT
	cmd.Params = builder.params
	return
}

func ZMPop(numKey int64) ZMPOPBuilder {
	return ZMPOPBuilder{
		params: []string{fmt.Sprintf("%d", numKey)},
	}
}

type ZMPOPBuilder struct {
	params []string
}

func (builder ZMPOPBuilder) Key(keys ...string) ZMPOPBuilder {
	for _, key := range keys {
		builder.params = append(builder.params, fmt.Sprintf("KEY:%s", key))
	}
	return builder
}

func (builder ZINTERSTOREBuilder) Max() ZINTERSTOREBuilder {
	builder.params = append(builder.params, "MAX")
	return builder
}

func (builder ZINTERSTOREBuilder) Min() ZINTERSTOREBuilder {
	builder.params = append(builder.params, "MIN")
	return builder
}

func (builder ZMPOPBuilder) Count(count int64) ZMPOPBuilder {
	builder.params = append(builder.params, fmt.Sprintf("COUNT:%d", count))
	return builder
}

func (builder ZMPOPBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZMPOP
	cmd.Params = builder.params
	return
}

func ZMScore(key string, members ...string) ZMSCOREBuilder {
	return ZMSCOREBuilder{
		params: append([]string{key}, members...),
	}
}

type ZMSCOREBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder ZMSCOREBuilder) CacheTTL(ttl time.Duration) ZMSCOREBuilder {
	builder.ttl = ttl
	return builder
}

func (builder ZMSCOREBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZMSCORE
	cmd.Params = builder.params
	return
}

func ZPopMax(key string) ZPOPMAXBuilder {
	return ZPOPMAXBuilder{
		params: append([]string{key}),
	}
}

type ZPOPMAXBuilder struct {
	params []string
}

func (builder ZPOPMAXBuilder) Count(count int64) ZPOPMAXBuilder {
	builder.params = append(builder.params, fmt.Sprintf("%d", count))
	return builder
}

func (builder ZPOPMAXBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZPOPMAX
	cmd.Params = builder.params
	return
}

func ZPopMin(key string) ZPOPMINBuilder {
	return ZPOPMINBuilder{
		params: append([]string{key}),
	}
}

type ZPOPMINBuilder struct {
	params []string
}

func (builder ZPOPMINBuilder) Count(count int64) ZPOPMINBuilder {
	builder.params = append(builder.params, fmt.Sprintf("%d", count))
	return builder
}

func (builder ZPOPMINBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZPOPMIN
	cmd.Params = builder.params
	return
}

func ZRandomMember(key string) ZRANDMEMBERBuilder {
	return ZRANDMEMBERBuilder{
		params: append([]string{key}),
	}
}

type ZRANDMEMBERBuilder struct {
	params []string
}

func (builder ZRANDMEMBERBuilder) Count(count int64) ZRANDMEMBERBuilder {
	builder.params = append(builder.params, fmt.Sprintf("%d", count))
	return builder
}

func (builder ZRANDMEMBERBuilder) Withscores() ZRANDMEMBERBuilder {
	builder.params = append(builder.params, "WITHSCORES")
	return builder
}

func (builder ZRANDMEMBERBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZRANDMEMBER
	cmd.Params = builder.params
	return
}

func ZRange(key string, min string, max string) ZRANGEBuilder {
	return ZRANGEBuilder{
		params: append([]string{key, min, max}),
	}
}

type ZRANGEBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder ZRANGEBuilder) ByScore() ZRANGEBuilder {
	builder.params = append(builder.params, "BYSCORE")
	return builder
}

func (builder ZRANGEBuilder) ByLex() ZRANGEBuilder {
	builder.params = append(builder.params, "BYLEX")
	return builder
}

func (builder ZRANGEBuilder) Rev() ZRANGEBuilder {
	builder.params = append(builder.params, "REV")
	return builder
}

func (builder ZRANGEBuilder) Limit(offset int64, limit int64) ZRANGEBuilder {
	builder.params = append(builder.params, fmt.Sprintf("LIMIT:%d,%d", offset, limit))
	return builder
}

func (builder ZRANGEBuilder) Withscores() ZRANGEBuilder {
	builder.params = append(builder.params, "WITHSCORES")
	return builder
}

func (builder ZRANGEBuilder) CacheTTL(ttl time.Duration) ZRANGEBuilder {
	builder.ttl = ttl
	return builder
}

func (builder ZRANGEBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZRANGE
	cmd.Params = builder.params
	return
}

func ZRangeStore(dst string, src string, key string, min string, max string) ZRANGESTOREBuilder {
	return ZRANGESTOREBuilder{
		params: append([]string{dst, src, key, min, max}),
	}
}

type ZRANGESTOREBuilder struct {
	params []string
}

func (builder ZRANGESTOREBuilder) ByScore() ZRANGESTOREBuilder {
	builder.params = append(builder.params, "BYSCORE")
	return builder
}

func (builder ZRANGESTOREBuilder) ByLex() ZRANGESTOREBuilder {
	builder.params = append(builder.params, "BYLEX")
	return builder
}

func (builder ZRANGESTOREBuilder) Rev() ZRANGESTOREBuilder {
	builder.params = append(builder.params, "REV")
	return builder
}

func (builder ZRANGESTOREBuilder) Limit(offset int64, limit int64) ZRANGESTOREBuilder {
	builder.params = append(builder.params, fmt.Sprintf("LIMIT:%d,%d", offset, limit))
	return builder
}

func (builder ZRANGESTOREBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZRANGESTORE
	cmd.Params = builder.params
	return
}

func ZRank(key string, member string) ZRANKBuilder {
	return ZRANKBuilder{
		params: []string{key, member},
	}
}

type ZRANKBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder ZRANKBuilder) Withscores() ZRANKBuilder {
	builder.params = append(builder.params, "WITHSCORES")
	return builder
}

func (builder ZRANKBuilder) CacheTTL(ttl time.Duration) ZRANKBuilder {
	builder.ttl = ttl
	return builder
}

func (builder ZRANKBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZRANK
	cmd.Params = builder.params
	return
}

func ZRem(key string, members ...string) ZREMBuilder {
	return ZREMBuilder{
		params: append([]string{key}, members...),
	}
}

type ZREMBuilder struct {
	params []string
}

func (builder ZREMBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZREM
	cmd.Params = builder.params
	return
}

func ZRemRangeByLex(key string, min string, max string) ZREMRANGEBYLEXBuilder {
	return ZREMRANGEBYLEXBuilder{
		params: []string{key, min, max},
	}
}

type ZREMRANGEBYLEXBuilder struct {
	params []string
}

func (builder ZREMRANGEBYLEXBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZREMRANGEBYLEX
	cmd.Params = builder.params
	return
}

func ZRemRangeByRank(key string, start int64, stop int64) ZREMRANGEBYRANKBuilder {
	return ZREMRANGEBYRANKBuilder{
		params: []string{key, strconv.FormatInt(start, 10), strconv.FormatInt(stop, 10)},
	}
}

type ZREMRANGEBYRANKBuilder struct {
	params []string
}

func (builder ZREMRANGEBYRANKBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZREMRANGEBYRANK
	cmd.Params = builder.params
	return
}

func ZRemRangeByScore(key string, min string, max string) ZREMRANGEBYSCOREBuilder {
	return ZREMRANGEBYSCOREBuilder{
		params: []string{key, min, max},
	}
}

type ZREMRANGEBYSCOREBuilder struct {
	params []string
}

func (builder ZREMRANGEBYSCOREBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZREMRANGEBYSCORE
	cmd.Params = builder.params
	return
}

func ZRevRank(key string, member string) ZREVRANKBuilder {
	return ZREVRANKBuilder{
		params: []string{key, member},
	}
}

type ZREVRANKBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder ZREVRANKBuilder) Withscores() ZREVRANKBuilder {
	builder.params = append(builder.params, "WITHSCORES")
	return builder
}

func (builder ZREVRANKBuilder) CacheTTL(ttl time.Duration) ZREVRANKBuilder {
	builder.ttl = ttl
	return builder
}

func (builder ZREVRANKBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZREVRANK
	cmd.Params = builder.params
	return
}

func ZScan(key string, cursor uint64) ZSCANBuilder {
	return ZSCANBuilder{
		params: []string{key, strconv.FormatUint(cursor, 10)},
	}
}

type ZSCANBuilder struct {
	params []string
}

func (builder ZSCANBuilder) Match(pattern string) ZSCANBuilder {
	builder.params = append(builder.params, fmt.Sprintf("MATCH:%s", pattern))
	return builder
}

func (builder ZSCANBuilder) Count(count int64) ZSCANBuilder {
	builder.params = append(builder.params, fmt.Sprintf("COUNT:%d", count))
	return builder
}

func (builder ZSCANBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZSCAN
	cmd.Params = builder.params
	return
}

func Zscore(key string, member string) ZSCOREBuilder {
	return ZSCOREBuilder{
		params: []string{key, member},
	}
}

type ZSCOREBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder ZSCOREBuilder) CacheTTL(ttl time.Duration) ZSCOREBuilder {
	builder.ttl = ttl
	return builder
}

func (builder ZSCOREBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZSCORE
	cmd.Params = builder.params
	return
}

func ZUnion(numKey int64) ZUNIONBuilder {
	return ZUNIONBuilder{
		params: []string{fmt.Sprintf("%d", numKey)},
	}
}

type ZUNIONBuilder struct {
	params []string
}

func (builder ZUNIONBuilder) Key(keys ...string) ZUNIONBuilder {
	for _, key := range keys {
		builder.params = append(builder.params, fmt.Sprintf("KEY:%s", key))
	}
	return builder
}

func (builder ZUNIONBuilder) Weights(weights ...int64) ZUNIONBuilder {
	for _, weight := range weights {
		builder.params = append(builder.params, fmt.Sprintf("WEIGHT:%d", weight))
	}
	return builder
}

func (builder ZUNIONBuilder) AggregateMax() ZUNIONBuilder {
	builder.params = append(builder.params, "AGGMAX")
	return builder
}

func (builder ZUNIONBuilder) AggregateMin() ZUNIONBuilder {
	builder.params = append(builder.params, "AGGMIX")
	return builder
}

func (builder ZUNIONBuilder) AggregateSum() ZUNIONBuilder {
	builder.params = append(builder.params, "AGGSUM")
	return builder
}

func (builder ZUNIONBuilder) Withscores() ZUNIONBuilder {
	builder.params = append(builder.params, "WITHSCORES")
	return builder
}

func (builder ZUNIONBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZUNION
	cmd.Params = builder.params
	return
}

func ZUnionStore(dst string, numKey int64) ZUNIONSTOREBuilder {
	return ZUNIONSTOREBuilder{
		params: []string{dst, fmt.Sprintf("%d", numKey)},
	}
}

type ZUNIONSTOREBuilder struct {
	params []string
}

func (builder ZUNIONSTOREBuilder) Key(keys ...string) ZUNIONSTOREBuilder {
	for _, key := range keys {
		builder.params = append(builder.params, fmt.Sprintf("KEY:%s", key))
	}
	return builder
}

func (builder ZUNIONSTOREBuilder) Weights(weights ...int64) ZUNIONSTOREBuilder {
	for _, weight := range weights {
		builder.params = append(builder.params, fmt.Sprintf("WEIGHT:%d", weight))
	}
	return builder
}

func (builder ZUNIONSTOREBuilder) AggregateMax() ZUNIONSTOREBuilder {
	builder.params = append(builder.params, "AGGMAX")
	return builder
}

func (builder ZUNIONSTOREBuilder) AggregateMin() ZUNIONSTOREBuilder {
	builder.params = append(builder.params, "AGGMIX")
	return builder
}

func (builder ZUNIONSTOREBuilder) AggregateSum() ZUNIONSTOREBuilder {
	builder.params = append(builder.params, "AGGSUM")
	return builder
}

func (builder ZUNIONSTOREBuilder) Build() (cmd Command) {
	cmd.Name = cmds.ZUNIONSTORE
	cmd.Params = builder.params
	return
}

func Z() SortedSet {
	return SortedSet{}
}

type SortedSet struct {
}

func (s SortedSet) BZMPop(timeout float64, numKeys int64) BZMPOPBuilder {
	return BZMPop(timeout, numKeys)
}

func (s SortedSet) BZPopMax(timeout float64, keys ...string) BZPOPMAXBuilder {
	return BZPopMax(timeout, keys...)
}

func (s SortedSet) Add(key string) ZADDBuilder {
	return ZAdd(key)
}

func (s SortedSet) Card(key string) ZCARDBuilder {
	return ZCard(key)
}

func (s SortedSet) Count(key string, min string, max string) ZCOUNTBuilder {
	return ZCount(key, min, max)
}

func (s SortedSet) Diff(numKey int64) ZDIFFBuilder {
	return ZDiff(numKey)
}

func (s SortedSet) DiffStore(dst string, numKey int64, keys ...string) ZDIFFSTOREBuilder {
	return ZDiffStore(dst, numKey, keys...)
}

func (s SortedSet) Inter(numKey int64) ZINTERBuilder {
	return ZInter(numKey)
}

func (s SortedSet) InterCard(numKey int64) ZINTERCARDBuilder {
	return ZInterCard(numKey)
}

func (s SortedSet) InterStore(dst string, numKey int64) ZINTERSTOREBuilder {
	return ZInterStore(dst, numKey)
}

func (s SortedSet) LexCount(key string, min string, max string) ZLEXCOUNTBuilder {
	return ZLexCount(key, min, max)
}

func (s SortedSet) MPop(numKey int64) ZMPOPBuilder {
	return ZMPop(numKey)
}

func (s SortedSet) MScore(key string, members ...string) ZMSCOREBuilder {
	return ZMScore(key, members...)
}

func (s SortedSet) PopMax(key string) ZPOPMAXBuilder {
	return ZPopMax(key)
}

func (s SortedSet) PopMin(key string) ZPOPMINBuilder {
	return ZPopMin(key)
}

func (s SortedSet) RandomMember(key string) ZRANDMEMBERBuilder {
	return ZRandomMember(key)
}

func (s SortedSet) Range(key string, min string, max string) ZRANGEBuilder {
	return ZRange(key, min, max)
}

func (s SortedSet) RangeStore(dst string, src string, key string, min string, max string) ZRANGESTOREBuilder {
	return ZRangeStore(dst, src, key, min, max)
}

func (s SortedSet) Rank(key string, member string) ZRANKBuilder {
	return ZRank(key, member)
}

func (s SortedSet) Rem(key string, members ...string) ZREMBuilder {
	return ZRem(key, members...)
}

func (s SortedSet) RemRangeByLex(key string, min string, max string) ZREMRANGEBYLEXBuilder {
	return ZRemRangeByLex(key, min, max)
}

func (s SortedSet) RemRangeByRank(key string, start int64, stop int64) ZREMRANGEBYRANKBuilder {
	return ZRemRangeByRank(key, start, stop)
}

func (s SortedSet) RemRangeByScore(key string, min string, max string) ZREMRANGEBYSCOREBuilder {
	return ZRemRangeByScore(key, min, max)
}

func (s SortedSet) RevRank(key string, member string) ZREVRANKBuilder {
	return ZRevRank(key, member)
}

func (s SortedSet) Scan(key string, cursor uint64) ZSCANBuilder {
	return ZScan(key, cursor)
}

func (s SortedSet) Score(key string, member string) ZSCOREBuilder {
	return Zscore(key, member)
}

func (s SortedSet) Union(numKey int64) ZUNIONBuilder {
	return ZUnion(numKey)
}

func (s SortedSet) UnionStore(dst string, numKey int64) ZUNIONSTOREBuilder {
	return ZUnionStore(dst, numKey)
}
