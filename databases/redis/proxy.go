package redis

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/json"
)

const (
	requestLocalPipelineHostId = "@fns_redis_pid"
	redisOptionsContextKey     = "@fns_redis_options"
)

var (
	defaultProxyOptions = &ProxyOptions{
		database: "",
	}
)

type ProxyOption func(*ProxyOptions)

type ProxyOptions struct {
	database string
}

func newDefaultProxyOptions() *ProxyOptions {
	return &ProxyOptions{
		database: "",
	}
}

func Database(name string) ProxyOption {
	return func(options *ProxyOptions) {
		options.database = name
	}
}

func WithOptions(ctx context.Context, options ...ProxyOption) context.Context {
	opt := newDefaultProxyOptions()
	if options != nil {
		for _, option := range options {
			option(opt)
		}
	}
	return context.WithValue(ctx, redisOptionsContextKey, opt)
}

func getOptions(ctx context.Context) (options *ProxyOptions) {
	v := ctx.Value(redisOptionsContextKey)
	if v == nil {
		options = defaultProxyOptions
		return
	}
	options = v.(*ProxyOptions)
	return
}

func newProxyParam(database string, param interface{}) (p *proxyParam, err error) {
	payload, encodeErr := json.Marshal(param)
	if encodeErr != nil {
		err = errors.Warning("redis: encode param failed").WithCause(encodeErr)
		return
	}
	p = &proxyParam{
		Database: database,
		Payload:  payload,
	}
	return
}

type proxyParam struct {
	Database string          `json:"database"`
	Payload  json.RawMessage `json:"payload"`
}

func (p *proxyParam) ScanPayload(v interface{}) (err error) {
	err = json.Unmarshal(p.Payload, v)
	if err != nil {
		err = errors.Warning("redis: decode param failed").WithCause(err)
		return
	}
	return
}
