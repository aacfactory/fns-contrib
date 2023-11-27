package dialect

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/postgres/dialect/deletes"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/postgres/dialect/inserts"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/postgres/dialect/selects"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/postgres/dialect/updates"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
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
}

func (generics *Generics) Get(spec *specifications.Specification) (generic *Generic, has bool) {
	v, exist := generics.values.Load(spec.Key)
	if exist {
		generic, has = v.(*Generic)
	}
	return
}

func (generics *Generics) Set(spec *specifications.Specification, generic *Generic) {
	generics.values.Store(spec.Key, generics)
	return
}
