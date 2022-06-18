# Postgres ORM
## Features
* mapping use json in one request
* commons query functions
* support virtual column
* support conflict column
## Usage
### Define object reference mapping
```go
type Bar struct {
	Id    string `col:"ID,pk"`
	Name  string `col:"NAME"`
	FooId string `col:"FOO_ID"`
}

func (t Bar) TableName() (string, string) {
	return "schema", "bar_table_name"
}

type Foo struct {
    Id       string    `col:"ID,pk"`
    CreateBY string    `col:"CREATE_BY,acb"`
    CreateAT time.Time `col:"CREATE_AT,act"`
    ModifyBY string    `col:"MODIFY_BY,amb"`
    ModifyAT time.Time `col:"MODIFY_AT,amt"`
    DeleteBY string    `col:"DELETE_BY,adb"`
    DeleteAT time.Time `col:"DELETE_AT,adt"`
    Version  int64     `col:"VERSION,aol"`
    Name     string    `col:"NAME,+conflict"`
    Integer  int       `col:"INTEGER"`
    Double   float64   `col:"DOUBLE"`
    Bool     bool      `col:"BOOL"`
    Time     time.Time `col:"TIME"`
    JsonRaw  *Sample   `col:"JSON_RAW,json"`
    Bars     []*Baz    `col:"BAZS,links,ID+FOO_ID,ID DESC,0:10"`
    BarsNum  int       `col:"BARS_NUM,vc,SELECT COUNT(1) FROM \"schema\".\"bar_table_name\" WHERE \"FOO_ID\" = \"schema\".\"foo_table_name\".\"ID\"`
}

func (t Foo) TableName() (string, string) {
    return "schema", "foo_table_name"
}
```
### CRUD
```go
postgres.Insert(ctx, &foo{})
postgres.Query(ctx, postgres.NewConditions(postgres.Eq("id", "FOO")), &[]*foo{})
```