package sequences

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/mysql/dialect"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/valyala/bytebufferpool"
)

func Next(ctx context.Context, key string) (n int64, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	_, _ = buf.Write(specifications.SELECT)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write([]byte("nextval"))
	_, _ = buf.Write(specifications.LB)
	_, _ = buf.Write([]byte("'"))
	_, _ = buf.Write(bytex.FromString(key))
	_, _ = buf.Write([]byte("'"))
	_, _ = buf.Write(specifications.RB)
	query := buf.Bytes()
	rows, queryErr := sql.Query(ctx, query)
	if queryErr != nil {
		err = errors.Warning("mysql: next sequence value failed").WithCause(queryErr)
		return
	}
	if rows.Next() {
		scanErr := rows.Scan(&n)
		if scanErr != nil {
			_ = rows.Close()
			err = errors.Warning("mysql: next sequence value failed").WithCause(scanErr)
			return
		}
	}
	_ = rows.Close()
	return
}

var (
	currentQuery = []byte("SELECT `value` FROM `sequence` WHERE `name` = ?")
)

func Current(ctx context.Context, key string) (n int64, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	rows, queryErr := sql.Query(ctx, currentQuery, key)
	if queryErr != nil {
		err = errors.Warning("mysql: current sequence value failed").WithCause(queryErr)
		return
	}
	if rows.Next() {
		scanErr := rows.Scan(&n)
		if scanErr != nil {
			_ = rows.Close()
			err = errors.Warning("mysql: current sequence value failed").WithCause(scanErr)
			return
		}
	}
	_ = rows.Close()
	return
}
