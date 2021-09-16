package sql

import (
	"fmt"
	"math"
	"testing"
	"time"
)

type FooRow struct {
	Id       string    `col:"ID,PK"`
	CreateBY string    `col:"CREATE_BY,CREATE_BY"`
	CreateAT time.Time `col:"CREATE_AT,CREATE_AT"`
	ModifyBY string    `col:"MODIFY_BY,MODIFY_BY"`
	ModifyAT time.Time `col:"MODIFY_AT,MODIFY_AT"`
	//DeleteBY string     `col:"DELETE_BY,DELETE_BY"`
	//DeleteAT time.Time  `col:"DELETE_AT,DELETE_AT"`
	Version  int64      `col:"VERSION,VERSION"`
	Kind     string     `col:"KIND"`
	Name     string     `col:"NAME"`
	Phase    int        `col:"PHASE"`
	Bar      *BarRow    `col:"BAR,FK"`
	BazList  []*BazRow  `col:"-,LK" ref:"Id,FOO_ID" sort:"ID,CREATE_AT DESC"`
	ManyList []*ManyRow `col:"-,LK" ref:"Id, MANY_ID)"`
	Likes    int        `col:"LIKES,VC" src:"select count(1) from \"FNS\".\"FOO_LIKE\" where \"FOO_ID\" = \"S\".\"ID\""`
}

func (f FooRow) Table() (string, string, string) {
	return "FNS", "FOO", "S"
}

type BarRow struct {
	Id       string    `col:"ID,PK"`
	CreateBY string    `col:"CREATE_BY,CREATE_BY"`
	CreateAT time.Time `col:"CREATE_AT,CREATE_AT"`
	ModifyBY string    `col:"MODIFY_BY,MODIFY_BY"`
	ModifyAT time.Time `col:"MODIFY_AT,MODIFY_AT"`
	DeleteBY string    `col:"DELETE_BY,DELETE_BY"`
	DeleteAT time.Time `col:"DELETE_AT,DELETE_AT"`
	Version  int64     `col:"VERSION,VERSION"`
	Name     string    `col:"NAME"`
	Foo      *FooRow   `col:"FOO_ID,FK"`
}

func (f BarRow) Table() (string, string, string) {
	return "FNS", "BAR", "S"
}

type BazRow struct {
	Id       string    `col:"ID,PK"`
	CreateBY string    `col:"CREATE_BY,CREATE_BY"`
	CreateAT time.Time `col:"CREATE_AT,CREATE_AT"`
	ModifyBY string    `col:"MODIFY_BY,MODIFY_BY"`
	ModifyAT time.Time `col:"MODIFY_AT,MODIFY_AT"`
	DeleteBY string    `col:"DELETE_BY,DELETE_BY"`
	DeleteAT time.Time `col:"DELETE_AT,DELETE_AT"`
	Version  int64     `col:"VERSION,VERSION"`
	Name     string    `col:"NAME"`
	Foo      *FooRow   `col:"FOO_ID,FK"`
}

func (f BazRow) Table() (string, string, string) {
	return "FNS", "BAZ", "S"
}

type ManyRow struct {
	Id       string    `col:"ID,PK"`
	CreateBY string    `col:"CREATE_BY,CREATE_BY"`
	CreateAT time.Time `col:"CREATE_AT,CREATE_AT"`
	ModifyBY string    `col:"MODIFY_BY,MODIFY_BY"`
	ModifyAT time.Time `col:"MODIFY_AT,MODIFY_AT"`
	DeleteBY string    `col:"DELETE_BY,DELETE_BY"`
	DeleteAT time.Time `col:"DELETE_AT,DELETE_AT"`
	Version  int64     `col:"VERSION,VERSION"`
	Name     string    `col:"NAME"`
}

func (f ManyRow) Table() (string, string, string) {
	return "FNS", "MANY", "S"
}

func Test_TableInfo(t *testing.T) {

	v := &FooRow{}
	info := newTableInfo(v, "postgres")
	fmt.Println(fmt.Sprintf("%+v", info))
	fmt.Println(info.VirtualColumns[0].Source)

	fmt.Println(info.GetQuery.Query)
	fmt.Println(info.GetQuery.Params)

	fmt.Println(info.InsertQuery.Query)
	fmt.Println(info.InsertQuery.Params)

	fmt.Println(info.UpdateQuery.Query)
	fmt.Println(info.UpdateQuery.Params)

	fmt.Println(info.DeleteQuery.Query)
	fmt.Println(info.DeleteQuery.Params)

	fmt.Println(info.ExistQuery.Query)
	fmt.Println(info.ExistQuery.Params)

	fmt.Println(int(math.Ceil(float64(20) / float64(10))))
}

func TestDAO(t *testing.T) {

	d1 := DAO(&FooRow{})
	fmt.Println(d1)
	rows := make([]*FooRow, 0, 1)
	d2 := DAO(&rows)
	fmt.Println(d2)

}
