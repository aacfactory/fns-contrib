package sql

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
)

func BeginTransaction(ctx fns.Context) (err errors.CodeError) {
	err = BeginTransactionWithOption(ctx, DefaultTransactionOption())
	return
}

func BeginTransactionWithOption(ctx fns.Context, param BeginTransactionParam) (err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns SQL Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}

	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, txBeginFn, arg)

	result := TxAddress{}
	err = r.Get(ctx, &result)
	if err != nil {
		return
	}
	if ctx.App().ClusterMode() {
		ctx.Meta().SetExactProxyServiceAddress(namespace, result.Address)
	}
	return
}

func CommitTransaction(ctx fns.Context) (err errors.CodeError) {
	if ctx.App().ClusterMode() {
		_, has := ctx.Meta().GetExactProxyServiceAddress(namespace)
		if !has {
			err = errors.New(555, "***WARNING***", fmt.Sprintf("fns SQL Proxy: current context does not bind tx"))
			return
		}
	}

	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns SQL Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}

	arg, argErr := fns.NewArgument(fns.Empty{})
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, txCommitFn, arg)

	result := fns.Empty{}
	err = r.Get(ctx, &result)

	if ctx.App().ClusterMode() {
		ctx.Meta().DelExactProxyServiceAddress(namespace)
	}
	return
}

func RollbackTransaction(ctx fns.Context) (err errors.CodeError) {
	if ctx.App().ClusterMode() {
		_, has := ctx.Meta().GetExactProxyServiceAddress(namespace)
		if !has {
			err = errors.New(555, "***WARNING***", fmt.Sprintf("fns SQL Proxy: current context does not bind tx"))
			return
		}
	}

	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns SQL Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}

	arg, argErr := fns.NewArgument(fns.Empty{})
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, txRollbackFn, arg)

	result := fns.Empty{}
	err = r.Get(ctx, &result)

	if ctx.App().ClusterMode() {
		ctx.Meta().DelExactProxyServiceAddress(namespace)
	}
	return
}

func Query(ctx fns.Context, param Param) (rows *Rows, err errors.CodeError) {

	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns SQL Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}

	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, queryFn, arg)

	rows = &Rows{}
	err = r.Get(ctx, rows)

	return
}

func Execute(ctx fns.Context, param Param) (result *ExecResult, err errors.CodeError) {

	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns SQL Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}

	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, executeFn, arg)

	result = &ExecResult{}
	err = r.Get(ctx, result)

	return
}
