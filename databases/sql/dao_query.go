package sql

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
)

// select * from test a inner join (select id from test where val=4 limit 300000,5) b on a.id=b.id;
func (d *dao) Query(ctx fns.Context, param *QueryParam) (has bool, err errors.CodeError) {
	panic("implement me")
}
