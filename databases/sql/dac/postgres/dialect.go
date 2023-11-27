package postgres

import "github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"

const (
	dialectName = "postgres"
)

type Dialect struct {
}

func (dialect *Dialect) Name() string {
	return dialectName
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

func (dialect *Dialect) Insert(ctx specifications.Context, spec *specifications.Specification) (method specifications.Method, query []byte, arguments []any, err error) {
	//TODO implement me
	panic("implement me")
}

func (dialect *Dialect) InsertOrUpdate(ctx specifications.Context, spec *specifications.Specification) (method specifications.Method, query []byte, arguments []any, err error) {
	//TODO implement me
	panic("implement me")
}

func (dialect *Dialect) InsertWhenExist(ctx specifications.Context, spec *specifications.Specification, source string) (method specifications.Method, query []byte, arguments []any, err error) {
	//TODO implement me
	panic("implement me")
}

func (dialect *Dialect) InsertWhenNotExist(ctx specifications.Context, spec *specifications.Specification, source string) (method specifications.Method, query []byte, arguments []any, err error) {
	//TODO implement me
	panic("implement me")
}

func (dialect *Dialect) Update(ctx specifications.Context, spec *specifications.Specification) (method specifications.Method, query []byte, arguments []any, err error) {
	//TODO implement me
	panic("implement me")
}

func (dialect *Dialect) UpdateFields(ctx specifications.Context, spec *specifications.Specification, fields []specifications.FieldValue, cond specifications.Condition) (method specifications.Method, query []byte, arguments []any, err error) {
	//TODO implement me
	panic("implement me")
}

func (dialect *Dialect) Delete(ctx specifications.Context, spec *specifications.Specification) (method specifications.Method, query []byte, arguments []any, err error) {
	//TODO implement me
	panic("implement me")
}

func (dialect *Dialect) DeleteWithConditions(ctx specifications.Context, spec *specifications.Specification, cond specifications.Condition) (method specifications.Method, query []byte, arguments []any, err error) {
	//TODO implement me
	panic("implement me")
}

func (dialect *Dialect) Exist(ctx specifications.Context, spec *specifications.Specification, cond specifications.Condition) (method specifications.Method, query []byte, arguments []any, err error) {
	//TODO implement me
	panic("implement me")
}

func (dialect *Dialect) Count(ctx specifications.Context, spec *specifications.Specification, cond specifications.Condition) (method specifications.Method, query []byte, arguments []any, err error) {
	//TODO implement me
	panic("implement me")
}

func (dialect *Dialect) Select(ctx specifications.Context, spec *specifications.Specification, cond specifications.Condition, orders specifications.Orders, offset int, length int) (method specifications.Method, query []byte, arguments []any, err error) {
	//TODO implement me
	panic("implement me")
}
