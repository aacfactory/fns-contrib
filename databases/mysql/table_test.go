package mysql

import (
	"fmt"
	"testing"
)

func TestTable(t *testing.T) {
	foo := &Foo{}
	fooTable := createOrLoadTable(foo)
	fmt.Println(fooTable.TableName())
	fmt.Println("--")
	fmt.Println(fooTable.insertQuery.query)
	fmt.Println(len(fooTable.insertQuery.columns))
	fmt.Println(fooTable.insertQuery.columns)
	fmt.Println("--")
	fmt.Println(fooTable.updateQuery.query)
	fmt.Println(len(fooTable.updateQuery.columns))
	fmt.Println(fooTable.insertOrUpdateQuery)
	if fooTable.insertOrUpdateQuery != nil {
		fmt.Println("--")
		fmt.Println(fooTable.insertOrUpdateQuery.query)
		fmt.Println(len(fooTable.insertOrUpdateQuery.columns))
	}
	fmt.Println("--")
	fmt.Println(fooTable.deleteQuery.query)
	fmt.Println(len(fooTable.deleteQuery.columns))
	if fooTable.softDeleteQuery != nil {
		fmt.Println("--")
		fmt.Println(fooTable.softDeleteQuery.query)
		fmt.Println(len(fooTable.softDeleteQuery.columns))
	}
	fmt.Println("--")
	fmt.Println(fooTable.querySelects)
	fmt.Println("--")

	fmt.Println(fooTable.generateExistSQL(NewConditions(Eq("ID", 1))))
	fmt.Println(fooTable.generateCountSQL(NewConditions(Eq("ID", 1))))

	q, _ := fooTable.generateQuerySQL(NewConditions(Eq("ID", LitValue("'1'"))), NewRange(0, 10), NewOrders().Asc("ID").values)
	fmt.Println(q)
}

type Sample struct {
	Name string
}

type Foo struct {
	Id string `col:"ID,pk"`
	//CreateBY string    `col:"CREATE_BY,acb"`
	//CreateAT time.Time `col:"CREATE_AT,act"`
	//ModifyBY string    `col:"MODIFY_BY,amb"`
	//ModifyAT time.Time `col:"MODIFY_AT,amt"`
	//DeleteBY string    `col:"DELETE_BY,adb"`
	//DeleteAT time.Time `col:"DELETE_AT,adt"`
	//Version  int64     `col:"VERSION,aol"`
	Name    string `col:"NAME,+conflict"`
	Integer int    `col:"INTEGER"`
	//Double   float64   `col:"DOUBLE"`
	//Bool     bool      `col:"BOOL"`
	//Time     time.Time `col:"TIME"`
	//JsonRaw  *Sample   `col:"JSON_RAW,json"`
	BazList []*Baz `col:"BAZ_LIST,links,ID+FOO_ID,ID DESC,0:10"`
	BarNum  int    `col:"BAR_NUM,vc,SELECT COUNT(1) FROM BAR WHERE FOO_ID = FOO.ID"`
}

func (f Foo) TableName() (string, string) {
	return "METAVOOO", "FOO"
}

type Bar struct {
	Id int64 `col:"ID,incrPk"`
	//CreateBY string    `col:"CREATE_BY,acb"`
	//CreateAT time.Time `col:"CREATE_AT,act"`
	//ModifyBY string    `col:"MODIFY_BY,amb"`
	//ModifyAT time.Time `col:"MODIFY_AT,amt"`
	//DeleteBY string    `col:"DELETE_BY,adb"`
	//DeleteAT time.Time `col:"DELETE_AT,adt"`
	//Version  int64     `col:"VERSION,aol"`
	Name string `col:"NAME"`
	Foo  *Foo   `col:"FOO,ref,FOO_ID+ID"`
}

func (f Bar) TableName() (string, string) {
	return "METAVOOO", "BAR"
}

type Baz struct {
	Id    string `col:"ID,pk"`
	Name  string `col:"NAME"`
	FooId string `col:"FOO_ID"`
}

func (f Baz) TableName() (string, string) {
	return "METAVOOO", "BAZ"
}
