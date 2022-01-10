# SQL

基于 fns.Service 实现的内部 SQL 服务，讲 sql 操作服务化，同时支持分布式事务。

## 安装

```shell
go get github.com/aacfactory/fns-contrib/databases/sql
```

## 使用

### 配置文件

* 单机
    * masterSlaverMode = false，dsn 列表为一个元素。
* 主从
    * masterSlaverMode = true，dsn 列表第一个元素为主服务地址，后续为从服务地址。
* 集群
    * masterSlaverMode = false，dsn 列表多元素。

```json
{
  "sql": {
    "masterSlaverMode": false,
    "driver": "",
    "dsn": [
      "username:password@tcp(ip:port)/databases" // 也可以是 sql.Open() 中的参数值
    ],
    "maxIdles": 0,
    "maxOpens": 0
  }
}
```

### 导入驱动

fns.sql 本身不带驱动，需要导入与配置文件中相同的驱动。

```go
import _ "github.com/go-sql-driver/mysql"
```

### 服务部署

* fns为单机模式
    * 直接部署
* fns为分布式模式
    * 可以单独起一个（一组）只有 sql 服务的应用（推荐）。
    * 也可以与fns单机模式一样使用。
    * 支持分布式事务。

```go
app.Deply(sql.Service())
```
```go
// 手动标注方言，自动是以实际使用的driver进行标注。
app.RegisterDialect("postgres")
```
### 代理使用

具体参考 [proxy.go](https://github.com/aacfactory/fns-contrib/tree/main/databases/sql/proxy.go)
```go
// 在上下文中开启事务
sql.BeginTransaction(ctx)
// 提交上下文中的事务
sql.CommitTransaction(ctx)
// 查询，如果 param 中设置在事务中查询，则使用事务查询
sql.Query(ctx, param)
// 执行，如果 param 中设置在事务中查询，则使用事务查询
sql.Execute(ctx, param)
```
## 分布式事务（GlobalTransactionManagement）

使用以请求编号绑定事务，并在请求上下文中标记事务所在服务，在服务发现的精确发现功能中把同一个请求上下文（无论在哪个节点）都转发到事务所在服务。<br/>
注意事项：

* 事务开启时需求一个 timeout，默认是10秒，当在这个时间内没有被提交或回滚，超时后会自动回滚。
* 使用分布式事务的最佳方式是采样 proxy 中的函数，而非其它自行代理操作。
* 部署的方式最好是以单独服务的方式（一个fns内只有 sql 服务）部署一个集群。

## DAO (ORM)
fns.sql 提供一个 ORM 类型的 Database Access Object，支持二级缓存。
### 配置
cacheKind: 二级缓存的类型，默认是local。当为reids是，后续ttl为expire的时间（Duration格式）。
```json
{
  "sql": {
    "dao": {
      "cacheKind": "redis",
      "options":{
        "ttl": "30s"
      }
    }
  }
}
```
### 映射
```go
type UserRow struct {
	Id         string       `col:"ID,PK"` // PK，标识为主键
	CreateBY   string       `col:"CREATE_BY,ACB"` // ACB，创建人（如果设置，则当为空是自动使用上下文中的user id）
	CreateAT   time.Time    `col:"CREATE_AT,ACT"` // ACT，创建日期（如果设置，则当为空是自动使用当前时间）
	ModifyBY   string       `col:"MODIFY_BY,AMB"` // AMB，修改人（如果设置，则当为空是自动使用上下文中的user id）
	ModifyAT   time.Time    `col:"MODIFY_AT,AMT"` // AMT，修改日期（如果设置，则当为空是自动使用当前时间）
	DeleteBY   string       `col:"DELETE_BY,ADB"` // ADB，删除人（如果设置，则当为空是自动使用上下文中的user id）
	DeleteAT   time.Time    `col:"DELETE_AT,ADT"` // ADT，删除日期（如果设置，则当为空是自动使用当前时间）
	Version    int64        `col:"VERSION,OL"` // OL，乐观锁（如果设置，会自动处理）
	Name       string       `col:"NAME"`
	Password   string       `col:"PASSWORD"`
	Gender     string       `col:"GENDER"`
	Age        int          `col:"AGE"`
	Active     bool         `col:"ACTIVE"`
	SignUpTime time.Time    `col:"SIGN_UP_TIME"`
	Profile    *UserProfile `col:"PROFILE,JSON"` // JSON，自动编码转换
	Score      float64      `col:"SCORE"`
	DOB        time.Time    `col:"DOB"`
}

func (r *UserRow) Table() (string, string, string) {
	return "FNS", "USER", "U" // schema，table name，table alias
}

// json
type UserProfile struct {
    Name string `json:"name"`
    Age  int    `json:"age"`
}

type PostRow struct {
  Id       string            `col:"ID,PK"`
  CreateBY string            `col:"CREATE_BY,ACB"`
  CreateAT time.Time         `col:"CREATE_AT,ACT"`
  ModifyBY string            `col:"MODIFY_BY,AMB"`
  ModifyAT time.Time         `col:"MODIFY_AT,AMT"`
  Version  int64             `col:"VERSION,OL"`
  Title    string            `col:"TITLE"`
  Content  string            `col:"CONTENT"`
  Author   *UserRow          `col:"AUTHOR_ID,FK"` // FK，外键（当设置后，会自动读出，如果追加SYNC（FK:SYNC），会自动触发写操作）
  Likes    int               `col:"LIKES,VC" src:"SELECT COUNT(1) FROM \"FNS\".\"POST_LIKE\" WHERE \"POST_ID\" = \"P\".\"ID\" "` // VC，虚拟列
  Comments []*PostCommentRow `col:"COMMENTS,LK:SYNC" ref:"ID,POST_ID" sort:"CREATE_AT DESC"` // LK，一对多（当设置后，会自动读出，如果追加SYNC（LK:SYNC），会自动触发写操作）
}

func (r *PostRow) Table() (string, string, string) {
    return "FNS", "POST", "P"
}


type PostCommentRow struct {
  Id       string    `col:"ID,PK"`
  CreateBY string    `col:"CREATE_BY,ACB"`
  CreateAT time.Time `col:"CREATE_AT,ACT"`
  Post     *PostRow  `col:"POST_ID,FK"`
  User     *UserRow  `col:"USER_ID,FK"`
  Content  string    `col:"CONTENT"`
}

func (r *PostCommentRow) Table() (string, string, string) {
    return "FNS", "POST_COMMENT", "PC"
}
```

### 操作
获取DAO
```go
dao := sql.DAO(ctx)
```
使用DAO
```go
type DatabaseAccessObject interface {
	// 保存（Insert or Update）
    Save(ctx fns.Context, rows ...TableRow) (affected int, err errors.CodeError)
	// 插入
    Insert(ctx fns.Context, rows ...TableRow) (affected int, err errors.CodeError)
	// 更新
    Update(ctx fns.Context, rows ...TableRow) (affected int, err errors.CodeError)
	// 删除（当row设置ADB时，实际做更新操作）
    Delete(ctx fns.Context, rows ...TableRow) (affected int, err errors.CodeError)
	// 存在
    Exist(ctx fns.Context, row TableRow) (has bool, err errors.CodeError)
	// 获取
    Get(ctx fns.Context, row TableRow) (has bool, err errors.CodeError)
	// 查询
    Query(ctx fns.Context, param *QueryParam, rows interface{}) (has bool, err errors.CodeError)
    // 计数
	Count(ctx fns.Context, param *QueryParam, row TableRow) (num int, err errors.CodeError)
    // 分页
	Page(ctx fns.Context, param *QueryParam, rows interface{}) (page Paged, err errors.CodeError)
    // 清空一级缓存
	Close()
}
```