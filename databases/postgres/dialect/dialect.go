package dialect

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
	"golang.org/x/sync/singleflight"
	"sync"
)

const (
	Name = "postgres"
)

func NewDialect() *Dialect {
	return &Dialect{
		generics: &Generics{
			values: sync.Map{},
			group:  singleflight.Group{},
		},
	}
}

type Dialect struct {
	generics *Generics
}

func (dialect *Dialect) Name() string {
	return Name
}

func (dialect *Dialect) FormatIdent(ident []byte) []byte {
	identLen := len(ident)
	if identLen == 0 {
		return ident
	}
	if ident[0] == '"' {
		return ident
	}
	p := make([]byte, identLen+2)
	p[0] = '"'
	p[identLen+1] = '"'
	copy(p[1:], ident)
	return p
}

func (dialect *Dialect) QueryPlaceholder() specifications.QueryPlaceholder {
	return &Placeholder{}
}

func (dialect *Dialect) Insert(ctx specifications.Context, spec *specifications.Specification, values int) (method specifications.Method, query []byte, fields []int, returning []int, err error) {
	generic, has, getErr := dialect.generics.Get(ctx, spec)
	if getErr != nil {
		err = errors.Warning("sql: dialect generate insert failed").WithMeta("table", spec.Key).WithCause(getErr).WithMeta("dialect", Name)
		return
	}
	if !has {
		err = errors.Warning("sql: dialect generate insert failed").WithMeta("table", spec.Key).WithCause(fmt.Errorf("spec was not found")).WithMeta("dialect", Name)
		return
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	method, fields, returning, err = generic.Insert.Render(ctx, buf, values)
	if err != nil {
		err = errors.Warning("sql: dialect generate insert failed").WithMeta("table", spec.Key).WithCause(err).WithMeta("dialect", Name)
		return
	}
	query = buf.Bytes()
	return
}

func (dialect *Dialect) InsertOrUpdate(ctx specifications.Context, spec *specifications.Specification) (method specifications.Method, query []byte, fields []int, returning []int, err error) {
	generic, has, getErr := dialect.generics.Get(ctx, spec)
	if getErr != nil {
		err = errors.Warning("sql: dialect generate insert or update failed").WithMeta("table", spec.Key).WithCause(getErr).WithMeta("dialect", Name)
		return
	}
	if !has {
		err = errors.Warning("sql: dialect generate insert or update failed").WithMeta("table", spec.Key).WithCause(fmt.Errorf("spec was not found")).WithMeta("dialect", Name)
		return
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	method, fields, returning, err = generic.InsertOrUpdate.Render(ctx, buf)
	if err != nil {
		err = errors.Warning("sql: dialect generate insert or update failed").WithMeta("table", spec.Key).WithCause(err).WithMeta("dialect", Name)
		return
	}
	query = buf.Bytes()
	return
}

func (dialect *Dialect) InsertWhenExist(ctx specifications.Context, spec *specifications.Specification, src specifications.QueryExpr) (method specifications.Method, query []byte, fields []int, arguments []any, returning []int, err error) {
	generic, has, getErr := dialect.generics.Get(ctx, spec)
	if getErr != nil {
		err = errors.Warning("sql: dialect generate insert when exist failed").WithMeta("table", spec.Key).WithCause(getErr).WithMeta("dialect", Name)
		return
	}
	if !has {
		err = errors.Warning("sql: dialect generate insert when exist failed").WithMeta("table", spec.Key).WithCause(fmt.Errorf("spec was not found")).WithMeta("dialect", Name)
		return
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	method, fields, arguments, returning, err = generic.InsertWhenExist.Render(ctx, buf, src)
	if err != nil {
		err = errors.Warning("sql: dialect generate insert when exist failed").WithMeta("table", spec.Key).WithCause(err).WithMeta("dialect", Name)
		return
	}
	query = buf.Bytes()
	return
}

func (dialect *Dialect) InsertWhenNotExist(ctx specifications.Context, spec *specifications.Specification, src specifications.QueryExpr) (method specifications.Method, query []byte, fields []int, arguments []any, returning []int, err error) {
	generic, has, getErr := dialect.generics.Get(ctx, spec)
	if getErr != nil {
		err = errors.Warning("sql: dialect generate insert when not exist failed").WithMeta("table", spec.Key).WithCause(getErr).WithMeta("dialect", Name)
		return
	}
	if !has {
		err = errors.Warning("sql: dialect generate insert when not exist failed").WithMeta("table", spec.Key).WithCause(fmt.Errorf("spec was not found")).WithMeta("dialect", Name)
		return
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	method, fields, arguments, returning, err = generic.InsertWhenNotExist.Render(ctx, buf, src)
	if err != nil {
		err = errors.Warning("sql: dialect generate insert when not exist failed").WithMeta("table", spec.Key).WithCause(err).WithMeta("dialect", Name)
		return
	}
	query = buf.Bytes()
	return
}

func (dialect *Dialect) Update(ctx specifications.Context, spec *specifications.Specification) (method specifications.Method, query []byte, fields []int, err error) {
	generic, has, getErr := dialect.generics.Get(ctx, spec)
	if getErr != nil {
		err = errors.Warning("sql: dialect generate update failed").WithMeta("table", spec.Key).WithCause(getErr).WithMeta("dialect", Name)
		return
	}
	if !has {
		err = errors.Warning("sql: dialect generate update failed").WithMeta("table", spec.Key).WithCause(fmt.Errorf("spec was not found")).WithMeta("dialect", Name)
		return
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	method, fields, err = generic.Update.Render(ctx, buf)
	if err != nil {
		err = errors.Warning("sql: dialect generate update failed").WithMeta("table", spec.Key).WithCause(err).WithMeta("dialect", Name)
		return
	}
	query = buf.Bytes()
	return
}

func (dialect *Dialect) UpdateFields(ctx specifications.Context, spec *specifications.Specification, fields []specifications.FieldValue, cond specifications.Condition) (method specifications.Method, query []byte, arguments []any, err error) {
	generic, has, getErr := dialect.generics.Get(ctx, spec)
	if getErr != nil {
		err = errors.Warning("sql: dialect generate update fields failed").WithMeta("table", spec.Key).WithCause(getErr).WithMeta("dialect", Name)
		return
	}
	if !has {
		err = errors.Warning("sql: dialect generate update fields failed").WithMeta("table", spec.Key).WithCause(fmt.Errorf("spec was not found")).WithMeta("dialect", Name)
		return
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	method, arguments, err = generic.UpdateFields.Render(ctx, buf, fields, cond)
	if err != nil {
		err = errors.Warning("sql: dialect generate update fields failed").WithMeta("table", spec.Key).WithCause(err).WithMeta("dialect", Name)
		return
	}
	query = buf.Bytes()
	return
}

func (dialect *Dialect) Delete(ctx specifications.Context, spec *specifications.Specification) (method specifications.Method, query []byte, fields []int, err error) {
	generic, has, getErr := dialect.generics.Get(ctx, spec)
	if getErr != nil {
		err = errors.Warning("sql: dialect generate delete failed").WithMeta("table", spec.Key).WithCause(getErr).WithMeta("dialect", Name)
		return
	}
	if !has {
		err = errors.Warning("sql: dialect generate delete failed").WithMeta("table", spec.Key).WithCause(fmt.Errorf("spec was not found")).WithMeta("dialect", Name)
		return
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	method, fields, err = generic.Delete.Render(ctx, buf)
	if err != nil {
		err = errors.Warning("sql: dialect generate delete failed").WithMeta("table", spec.Key).WithCause(err).WithMeta("dialect", Name)
		return
	}
	query = buf.Bytes()
	return
}

func (dialect *Dialect) DeleteByConditions(ctx specifications.Context, spec *specifications.Specification, cond specifications.Condition) (method specifications.Method, query []byte, audits []int, arguments []any, err error) {
	generic, has, getErr := dialect.generics.Get(ctx, spec)
	if getErr != nil {
		err = errors.Warning("sql: dialect generate delete by conditions failed").WithMeta("table", spec.Key).WithCause(getErr).WithMeta("dialect", Name)
		return
	}
	if !has {
		err = errors.Warning("sql: dialect generate delete by conditions failed").WithMeta("table", spec.Key).WithCause(fmt.Errorf("spec was not found")).WithMeta("dialect", Name)
		return
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	method, audits, arguments, err = generic.DeleteByConditions.Render(ctx, buf, cond)
	if err != nil {
		err = errors.Warning("sql: dialect generate delete by conditions failed").WithMeta("table", spec.Key).WithCause(err).WithMeta("dialect", Name)
		return
	}
	query = buf.Bytes()
	return
}

func (dialect *Dialect) Exist(ctx specifications.Context, spec *specifications.Specification, cond specifications.Condition) (method specifications.Method, query []byte, arguments []any, err error) {
	generic, has, getErr := dialect.generics.Get(ctx, spec)
	if getErr != nil {
		err = errors.Warning("sql: dialect generate exist failed").WithMeta("table", spec.Key).WithCause(getErr).WithMeta("dialect", Name)
		return
	}
	if !has {
		err = errors.Warning("sql: dialect generate exist failed").WithMeta("table", spec.Key).WithCause(fmt.Errorf("spec was not found")).WithMeta("dialect", Name)
		return
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	method, arguments, err = generic.Exist.Render(ctx, buf, cond)
	if err != nil {
		err = errors.Warning("sql: dialect generate exist failed").WithMeta("table", spec.Key).WithCause(err).WithMeta("dialect", Name)
		return
	}
	query = buf.Bytes()
	return
}

func (dialect *Dialect) Count(ctx specifications.Context, spec *specifications.Specification, cond specifications.Condition) (method specifications.Method, query []byte, arguments []any, err error) {
	generic, has, getErr := dialect.generics.Get(ctx, spec)
	if getErr != nil {
		err = errors.Warning("sql: dialect generate count failed").WithMeta("table", spec.Key).WithCause(getErr).WithMeta("dialect", Name)
		return
	}
	if !has {
		err = errors.Warning("sql: dialect generate count failed").WithMeta("table", spec.Key).WithCause(fmt.Errorf("spec was not found")).WithMeta("dialect", Name)
		return
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	method, arguments, err = generic.Count.Render(ctx, buf, cond)
	if err != nil {
		err = errors.Warning("sql: dialect generate count failed").WithMeta("table", spec.Key).WithCause(err).WithMeta("dialect", Name)
		return
	}
	query = buf.Bytes()
	return
}

func (dialect *Dialect) Query(ctx specifications.Context, spec *specifications.Specification, cond specifications.Condition, orders specifications.Orders, groupBy specifications.GroupBy, having specifications.Having, offset int, length int) (method specifications.Method, query []byte, arguments []any, columns []int, err error) {
	generic, has, getErr := dialect.generics.Get(ctx, spec)
	if getErr != nil {
		err = errors.Warning("sql: dialect generate query failed").WithMeta("table", spec.Key).WithCause(getErr).WithMeta("dialect", Name)
		return
	}
	if !has {
		err = errors.Warning("sql: dialect generate query failed").WithMeta("table", spec.Key).WithCause(fmt.Errorf("spec was not found")).WithMeta("dialect", Name)
		return
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	method, arguments, columns, err = generic.Query.Render(ctx, buf, cond, orders, groupBy, having, offset, length)
	if err != nil {
		err = errors.Warning("sql: dialect generate query failed").WithMeta("table", spec.Key).WithCause(err).WithMeta("dialect", Name)
		return
	}
	query = buf.Bytes()
	return
}