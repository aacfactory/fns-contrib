package specifications

import (
	"context"
)

type Context interface {
	context.Context
	FormatIdent(ident []byte) []byte
	NextQueryPlaceholder() (v []byte)
	SkipNextQueryPlaceholderCursor(n int)
	// Localization
	// key can be struct field, struct value and [struct value, struct value]
	// when field then return column name
	// when value then return table name
	// when [struct value, struct value] then return column name of table name
	Localization(key any) (content [][]byte, has bool)
}

func Todo(ctx context.Context, key any, dialect Dialect) Context {
	return &renderCtx{
		Context: ctx,
		dialect: dialect,
		ph:      dialect.QueryPlaceholder(),
		key:     key,
	}
}

func Fork(ctx Context) Context {
	rc := ctx.(*renderCtx)
	return &renderCtx{
		Context: ctx,
		ph:      rc.getDialect().QueryPlaceholder(),
		key:     rc.key,
	}
}

func SwitchKey(ctx Context, key any) Context {
	return &renderCtx{
		Context: ctx,
		key:     key,
	}
}

type renderCtx struct {
	context.Context
	dialect Dialect
	ph      QueryPlaceholder
	key     any
}

func (ctx *renderCtx) getDialect() Dialect {
	if ctx.dialect != nil {
		return ctx.dialect
	}
	parent, ok := ctx.Context.(*renderCtx)
	if ok {
		return parent.getDialect()
	}
	return nil
}

func (ctx *renderCtx) FormatIdent(ident []byte) []byte {
	return ctx.getDialect().FormatIdent(ident)
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

func (ctx *renderCtx) SkipNextQueryPlaceholderCursor(n int) {
	if ctx.ph == nil {
		parent, ok := ctx.Context.(Context)
		if ok {
			parent.SkipNextQueryPlaceholderCursor(n)
		}
		return
	}
	ctx.ph.SkipCursor(n)
	return
}

func (ctx *renderCtx) Localization(key any) (content [][]byte, has bool) {
	sk, ok := key.(string)
	if ok {
		content, has = dict.Get(ctx.key, sk)
	} else {
		content, has = dict.Get(key)
	}
	if has {
		for i, c := range content {
			content[i] = ctx.getDialect().FormatIdent(c)
		}
	}
	return
}
