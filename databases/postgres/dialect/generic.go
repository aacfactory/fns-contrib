package dialect

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/postgres/dialect/deletes"
	"github.com/aacfactory/fns-contrib/databases/postgres/dialect/inserts"
	"github.com/aacfactory/fns-contrib/databases/postgres/dialect/selects"
	"github.com/aacfactory/fns-contrib/databases/postgres/dialect/updates"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"golang.org/x/sync/singleflight"
	"sync"
)

type Generic struct {
	Insert             *inserts.InsertGeneric
	InsertOrUpdate     *inserts.InsertOrUpdateGeneric
	InsertWhenExist    *inserts.InsertWhenExistsGeneric
	InsertWhenNotExist *inserts.InsertWhenNotExistsGeneric
	Update             *updates.UpdateGeneric
	UpdateFields       *updates.UpdateFieldsGeneric
	Delete             *deletes.DeleteGeneric
	DeleteByConditions *deletes.DeleteByConditionsGeneric
	Count              *selects.CountGeneric
	Exist              *selects.ExistGeneric
	Query              *selects.QueryGeneric
}

type Generics struct {
	values sync.Map
	group  singleflight.Group
}

func (generics *Generics) Get(ctx specifications.Context, spec *specifications.Specification) (generic *Generic, has bool, err error) {
	stored, exist := generics.values.Load(spec.Key)
	if exist {
		generic, has = stored.(*Generic)
		return
	}
	v, createErr, _ := generics.group.Do(spec.Key, func() (v interface{}, err error) {
		gen := &Generic{}
		gen.Insert, err = inserts.NewInsertGeneric(specifications.Fork(ctx), spec)
		if err != nil {
			return
		}
		gen.InsertOrUpdate, err = inserts.NewInsertOrUpdateGeneric(specifications.Fork(ctx), spec)
		if err != nil {
			return
		}
		gen.InsertWhenExist, err = inserts.NewInsertWhenExistsGeneric(specifications.Fork(ctx), spec)
		if err != nil {
			return
		}
		gen.InsertWhenNotExist, err = inserts.NewInsertWhenNotExistsGeneric(specifications.Fork(ctx), spec)
		if err != nil {
			return
		}
		gen.Update, err = updates.NewUpdateGeneric(specifications.Fork(ctx), spec)
		if err != nil {
			return
		}
		gen.UpdateFields, err = updates.NewUpdateFieldsGeneric(specifications.Fork(ctx), spec)
		if err != nil {
			return
		}
		gen.Delete, err = deletes.NewDeleteGeneric(specifications.Fork(ctx), spec)
		if err != nil {
			return
		}
		gen.DeleteByConditions, err = deletes.NewDeleteByConditionsGeneric(specifications.Fork(ctx), spec)
		if err != nil {
			return
		}
		gen.Count, err = selects.NewCountGeneric(specifications.Fork(ctx), spec)
		if err != nil {
			return
		}
		gen.Exist, err = selects.NewExistGeneric(specifications.Fork(ctx), spec)
		if err != nil {
			return
		}
		gen.Query, err = selects.NewQueryGeneric(specifications.Fork(ctx), spec)
		if err != nil {
			return
		}
		generics.values.Store(spec.Key, gen)
		v = gen
		return
	})
	if createErr != nil {
		err = errors.Warning("sql: get generic failed").WithCause(createErr).WithMeta("table", spec.Key)
		return
	}
	generic, has = v.(*Generic)
	return
}
