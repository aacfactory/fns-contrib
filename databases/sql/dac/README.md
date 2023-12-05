# DAC
It is a database access layer for sql.

## Define
### Table
Define a struct which implements `dac.Table`.  
Examples:  
User table.
```go
type User struct {
	Id          string                  `column:"ID,PK"`
	CreateBY    string                  `column:"CREATE_BY,ACB"`
	CreateAT    time.Time               `column:"CREATE_AT,ACT"`
	ModifyBY    string                  `column:"MODIFY_BY,AMB"`
	ModifyAT    time.Time               `column:"MODIFY_AT,AMT"`
	DeleteBY    string                  `column:"DELETE_BY,ADB"`
	DeleteAT    time.Time               `column:"DELETE_AT,ADT"`
	Version     int64                   `column:"VERSION,AOL"`
	Nickname    string                  `column:"NICKNAME"`
	Mobile      string                  `column:"MOBILE"`
	Gender      string                  `column:"GENDER"`
	Birthday    time.Time               `column:"BIRTHDAY"`
	Avatar      sql.NullJson[Avatar]    `column:"AVATAR,json"`
	BD          times.Date              `column:"BD"`
	BT          times.Time              `column:"BT"`
}

func (row User) TableInfo() dac.TableInfo {
	return dac.Info("USER", dac.Schema("FNS"))
}
```
User avatar json typed column
```go
// Avatar
// json column
type Avatar struct {
	Schema      string `json:"schema"`
	Domain      string `json:"domain"`
	Path        string `json:"path"`
	MimeType    string `json:"mimeType"`
	URL         string `json:"url"`
}
```
Post table.  
Author column is a reference column, which means many post to one user.  
Comments column is a links column, which means one post to many comments.  
Links columns is a virtual column, which means that column is select from another source.
```go
type Post struct {
	Id          string          `column:"ID,pk"`
	CreateBY    string          `column:"CREATE_BY,ACB"`
	CreateAT    time.Time       `column:"CREATE_AT,act"`
	Version     int64           `column:"VERSION,aol"`
	Author      User            `column:"AUTHOR,ref,Id"`
	Title       string          `column:"TITLE"`
	Content     string          `column:"CONTENT"`
	Comments    []PostComment   `column:"COMMENTS,links,Id+PostId,orders:Id@desc,length:10"`
	Likes       int64           `column:"LIKES,vc,basic,SELECT COUNT(1) FROM \"FNS\".\"POST_LIKE\" WHERE \"POST_ID\" = \"FNS\".\"POST\".\"ID\""`
}

func (row Post) TableInfo() dac.TableInfo {
	return dac.Info("POST", dac.Schema("FNS"))
}
```
Comment table.
```go
type PostComment struct {
	Id       int64      `column:"ID,pk,incr"`
	PostId   string     `column:"POST_ID"`
	User     User       `column:"USER_ID,ref,Id"`
	CreateAT time.Time  `column:"CREATE_AT,act"`
	Content  string     `column:"CONTENT"`
}

func (row PostComment) TableInfo() dac.TableInfo {
	return dac.Info("POST_COMMENT", dac.Schema("FNS"))
}
```
Like table.
```go
type PostLike struct {
	Id     int64  `column:"ID,pk,incr"`
	PostId string `column:"POST_ID"`
	UserId string `column:"USER_ID"`
}

func (row PostLike) TableInfo() dac.TableInfo {
	return dac.Info("POST_LIKE", dac.Schema("FNS"))
}
```
### View
There are two kinds view, one is pure view, another is projection (used for group by).  
Define a struct which implements `dac.View`.
Example:   
Count group genders.
```go
type UserGenderCount struct {
	Gender string `column:"GENDER"` // group by column
	Count  int64  `column:"ID,vc,agg,COUNT"` // agg column
}

func (u UserGenderCount) ViewInfo() dac.ViewInfo {
	return dac.TableView(User{}) // projection of User
}
```
### Column
Format of `column` tag is `{column name | ident},{kind},{options of kind}`.  
Kinds:
* normal: which is default.
* pk: primary key, when it is increment, then add `incr` option, such as `id,pk,incr`.
* acb: used for `create_by` column, only support `int` or `string` type.
* act: used for `create_at` column, only support `int` or `time` type.
* amb: used for `modify_by` column, only support `int` or `string` type.
* amt: used for `modify_at` column, only support `int` or `time` type.
* adb: used for `delete_by` column, only support `int` or `string` type.
* adt: used for `delete_at` column, only support `int` or `time` type.
* aol: used for `version` column, only support `int` type.
* ref: used for many to one or one to one. first option is `{target struct field name}`.
* link: used for one to one but host table has no target table column. first option is `{host struct field name}+{target struct field name}`.
* links: used for one to many. first option is `{host struct field name}+{target struct field name}`, when use order, then add order option, such as `orders:{field name}{@desc}`. when use limit, then add limit option, such as `length:{size}`.
* virtual: used for add out source column, first option is type of virtual. 
  * `basic` type means the column value is basic value. 
  * `object` type means the column value is one row which will be encoded by json.
  * `array` type means the column value is many rows which will be encoded by json.
  * `agg` type means the column value is result of aggregation.

### Note
* DON'T use ptr to implement Table or View.
* Anonymous field is supported, but can not be ptr and must be exported.
* When reference is not null, then use value, not use ptr, also as link.
* Element of links slice should be value not ptr.
* `InsertOrUpdate` only used for which table has conflict columns.
* When dialect does not support `returning`, then `InsertMulti` is not fully worked.

## Methods
* Insert
* InsertMulti
* InsertOrUpdate
* InsertWhenNotExist
* InsertWhenExist
* Update
* UpdateFields
* Delete
* DeleteByCondition
* Query
* One
* ALL
* Views
* ViewOne
* ViewALL