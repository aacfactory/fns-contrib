package conditions

import (
	"context"
)

type Context interface {
	context.Context
	NextQueryPlaceholder() (v []byte)
	// Localization
	// key can be struct field, struct value and [struct value, struct value]
	// when field then return column name
	// when value then return table name
	// when [struct value, struct value] then return column name of table name
	Localization(key any) (content []byte, has bool)
}

type Dict interface {
	Get(key ...any) (value []byte, has bool)
}

type QueryPlaceholder interface {
	Next() (v []byte)
}

func Todo(ctx context.Context, key any, dict Dict, ph QueryPlaceholder) Context {
	return &renderCtx{
		Context: ctx,
		ph:      ph,
		dict:    dict,
		key:     key,
	}
}

func With(ctx Context, key any) Context {
	return &renderCtx{
		Context: ctx,
		ph:      nil,
		dict:    nil,
		key:     key,
	}
}

type renderCtx struct {
	context.Context
	ph   QueryPlaceholder
	dict Dict
	key  any
}

func (ctx *renderCtx) NextQueryPlaceholder() (v []byte) {
	if ctx.ph == nil {
		parent, ok := ctx.Context.(Context)
		if ok {
			v = parent.NextQueryPlaceholder()
		}
		return
	}
	v = ctx.ph.Next()
	return
}

func (ctx *renderCtx) getDict() (dict Dict) {
	if ctx.dict == nil {
		parent, ok := ctx.Context.(*renderCtx)
		if ok {
			dict = parent.getDict()
		}
		return
	}
	dict = ctx.dict
	return
}

func (ctx *renderCtx) Localization(key any) (content []byte, has bool) {
	dict := ctx.getDict()
	sk, ok := key.(string)
	if ok {
		content, has = dict.Get(ctx.key, sk)
	} else {
		content, has = dict.Get(key)
	}
	return
}
