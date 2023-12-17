package redis

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
	"github.com/redis/rueidis"
)

type luaHandler struct {
	client rueidis.Client
}

func (handler *luaHandler) Name() string {
	return string(luaFnName)
}

func (handler *luaHandler) Internal() bool {
	return true
}

func (handler *luaHandler) Readonly() bool {
	return false
}

func (handler *luaHandler) Handle(ctx services.Request) (v any, err error) {
	param, paramErr := services.ValueOfParam[LuaParam](ctx.Param())
	if paramErr != nil {
		err = errors.Warning("redis: invalid param").WithCause(paramErr)
		return
	}
	var lua *rueidis.Lua
	if param.Readonly {
		lua = rueidis.NewLuaScriptReadOnly(param.Script)
	} else {
		lua = rueidis.NewLuaScript(param.Script)
	}
	resp := lua.Exec(ctx, handler.client, param.Keys, param.Args)
	v = newResult(resp)
	return
}

type LuaParam struct {
	Readonly bool     `json:"readonly"`
	Script   string   `json:"script"`
	Keys     []string `json:"keys"`
	Args     []string `json:"args"`
}

func ExecLua(ctx context.Context, script string, keys []string, args []string, readonly bool) (v Result, err error) {
	ep := used(ctx)
	if len(ep) == 0 {
		ep = endpointName
	}
	param := LuaParam{
		Readonly: readonly,
		Script:   script,
		Keys:     keys,
		Args:     args,
	}
	eps := runtime.Endpoints(ctx)
	response, handleErr := eps.Request(ctx, ep, luaFnName, param)
	if handleErr != nil {
		err = handleErr
		return
	}
	r := result{}
	err = response.Unmarshal(&r)
	if err != nil {
		return
	}
	v = r
	return
}
