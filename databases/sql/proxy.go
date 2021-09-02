package sql

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
)

func TxBegin(ctx fns.Context, param TxBeginParam) (err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns SQL Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}

	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, TxBeginFn, arg)

	result := TxAddress{}
	err = r.Get(ctx, &result)
	if err != nil {
		ctx.Meta().SetExactProxyService(Namespace, result.Address)
	}
	return
}

func TxCommit(ctx fns.Context) (err errors.CodeError) {
	_, _, has := ctx.Meta().GetExactProxyService()
	if !has {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns SQL Proxy: current context has not tx"))
		return
	}

	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns SQL Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}

	arg, argErr := fns.NewArgument(fns.Empty{})
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, TxCommitFn, arg)

	result := fns.Empty{}
	err = r.Get(ctx, &result)

	ctx.Meta().DelExactProxyService(Namespace)

	return
}

func TxRollback(ctx fns.Context) (err errors.CodeError) {
	_, _, has := ctx.Meta().GetExactProxyService()
	if !has {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns SQL Proxy: current context has not tx"))
		return
	}

	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns SQL Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}

	arg, argErr := fns.NewArgument(fns.Empty{})
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, TxCommitFn, arg)

	result := fns.Empty{}
	err = r.Get(ctx, &result)

	ctx.Meta().DelExactProxyService(Namespace)

	return
}

func Query(ctx fns.Context, param Param) (rows *Rows, err errors.CodeError) {

	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns SQL Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}

	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, QueryFn, arg)

	rows = &Rows{}
	err = r.Get(ctx, &rows)

	return
}

func Execute(ctx fns.Context, param Param) (result *ExecResult, err errors.CodeError) {

	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns SQL Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}

	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, ExecuteFn, arg)

	result = &ExecResult{}
	err = r.Get(ctx, &result)

	return
}
