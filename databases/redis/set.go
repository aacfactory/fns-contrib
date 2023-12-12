package redis

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/redis/cmds"
	"strconv"
	"time"
)

func SAdd(key string, members ...string) SADDBuilder {
	return SADDBuilder{
		params: append([]string{key}, members...),
	}
}

type SADDBuilder struct {
	params []string
}

func (builder SADDBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SADD
	cmd.Params = builder.params
	return
}

func SCard(key string) SCARDBuilder {
	return SCARDBuilder{
		params: []string{key},
	}
}

type SCARDBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder SCARDBuilder) CacheTTL(ttl time.Duration) SCARDBuilder {
	builder.ttl = ttl
	return builder
}

func (builder SCARDBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SCARD
	cmd.Params = builder.params
	return
}

func SDiff(key ...string) SDIFFBuilder {
	return SDIFFBuilder{
		params: key,
	}
}

type SDIFFBuilder struct {
	params []string
}

func (builder SDIFFBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SDIFF
	cmd.Params = builder.params
	return
}

func SDiffStore(dst string, key ...string) SDIFFSTOREBuilder {
	return SDIFFSTOREBuilder{
		params: append([]string{dst}, key...),
	}
}

type SDIFFSTOREBuilder struct {
	params []string
}

func (builder SDIFFSTOREBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SDIFFSTORE
	cmd.Params = builder.params
	return
}

func SInter(key ...string) SINTERBuilder {
	return SINTERBuilder{
		params: key,
	}
}

type SINTERBuilder struct {
	params []string
}

func (builder SINTERBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SINTER
	cmd.Params = builder.params
	return
}

func SInterCard(numKey int64, key ...string) SINTERCARDBuilder {
	params := []string{strconv.FormatInt(numKey, 10)}
	for _, k := range key {
		params = append(params, fmt.Sprintf("KEY:%s", k))
	}
	return SINTERCARDBuilder{
		params: params,
	}
}

type SINTERCARDBuilder struct {
	params []string
}

func (builder SINTERCARDBuilder) Limit(limit int64) SINTERCARDBuilder {
	builder.params = append(builder.params, fmt.Sprintf("LIMIT:%d", limit))
	return builder
}

func (builder SINTERCARDBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SINTERCARD
	cmd.Params = builder.params
	return
}

func SInterStore(dst string, key ...string) SINTERSTOREBuilder {
	return SINTERSTOREBuilder{
		params: append([]string{dst}, key...),
	}
}

type SINTERSTOREBuilder struct {
	params []string
}

func (builder SINTERSTOREBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SINTERSTORE
	cmd.Params = builder.params
	return
}

func SISMember(key string, member string) SINTERSTOREBuilder {
	return SINTERSTOREBuilder{
		params: []string{key, member},
	}
}

type SISMEMBERBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder SISMEMBERBuilder) CacheTTL(ttl time.Duration) SISMEMBERBuilder {
	builder.ttl = ttl
	return builder
}

func (builder SISMEMBERBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SISMEMBER
	cmd.Params = builder.params
	return
}

func SMembers(key string) SMEMBERSBuilder {
	return SMEMBERSBuilder{
		params: []string{key},
	}
}

type SMEMBERSBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder SMEMBERSBuilder) CacheTTL(ttl time.Duration) SMEMBERSBuilder {
	builder.ttl = ttl
	return builder
}

func (builder SMEMBERSBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SMEMBERS
	cmd.Params = builder.params
	return
}

func SMISMember(key string, members ...string) SMISMEMBERBuilder {
	return SMISMEMBERBuilder{
		params: append([]string{key}, members...),
	}
}

type SMISMEMBERBuilder struct {
	params []string
	ttl    time.Duration
}

func (builder SMISMEMBERBuilder) CacheTTL(ttl time.Duration) SMISMEMBERBuilder {
	builder.ttl = ttl
	return builder
}

func (builder SMISMEMBERBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SMISMEMBER
	cmd.Params = builder.params
	return
}

func SMove(src string, dst string, member string) SMOVEBuilder {
	return SMOVEBuilder{
		params: []string{src, dst, member},
	}
}

type SMOVEBuilder struct {
	params []string
}

func (builder SMOVEBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SMOVE
	cmd.Params = builder.params
	return
}

func SPop(key string) SPOPBuilder {
	return SPOPBuilder{
		params: []string{key},
	}
}

type SPOPBuilder struct {
	params []string
}

func (builder SPOPBuilder) Count(count int64) SPOPBuilder {
	builder.params = append(builder.params, fmt.Sprintf("COUNT:%d", count))
	return builder
}

func (builder SPOPBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SPOP
	cmd.Params = builder.params
	return
}

func SRandMember(key string) SRANDMEMBERBuilder {
	return SRANDMEMBERBuilder{
		params: []string{key},
	}
}

type SRANDMEMBERBuilder struct {
	params []string
}

func (builder SRANDMEMBERBuilder) Count(count int64) SRANDMEMBERBuilder {
	builder.params = append(builder.params, fmt.Sprintf("COUNT:%d", count))
	return builder
}

func (builder SRANDMEMBERBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SRANDMEMBER
	cmd.Params = builder.params
	return
}

func SRem(key string, members ...string) SRANDMEMBERBuilder {
	return SRANDMEMBERBuilder{
		params: append([]string{key}, members...),
	}
}

type SREMBuilder struct {
	params []string
}

func (builder SREMBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SREM
	cmd.Params = builder.params
	return
}

func SScan(key string, cursor uint64) SSCANBuilder {
	return SSCANBuilder{
		params: []string{key, strconv.FormatUint(cursor, 10)},
	}
}

type SSCANBuilder struct {
	params []string
}

func (builder SSCANBuilder) Match(pattern string) SSCANBuilder {
	builder.params = append(builder.params, fmt.Sprintf("MATCH:%s", pattern))
	return builder
}

func (builder SSCANBuilder) Count(count int64) SSCANBuilder {
	builder.params = append(builder.params, fmt.Sprintf("COUNT:%d", count))
	return builder
}

func (builder SSCANBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SSCAN
	cmd.Params = builder.params
	return
}

func SUnion(key ...string) SUNIONBuilder {
	return SUNIONBuilder{
		params: key,
	}
}

type SUNIONBuilder struct {
	params []string
}

func (builder SUNIONBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SUNION
	cmd.Params = builder.params
	return
}

func SUnionStore(dst string, key ...string) SUNIONSTOREBuilder {
	return SUNIONSTOREBuilder{
		params: append([]string{dst}, key...),
	}
}

type SUNIONSTOREBuilder struct {
	params []string
}

func (builder SUNIONSTOREBuilder) Build() (cmd Command) {
	cmd.Name = cmds.SUNIONSTORE
	cmd.Params = builder.params
	return
}
