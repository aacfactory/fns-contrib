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
      "username:password@tcp(ip:port)/databases"
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

### 代理使用

具体参考 [proxy.go](https://github.com/aacfactory/fns-contrib/tree/main/databases/sql/proxy.go)

## 分布式事务（GlobalTransactionManagement）

使用以请求编号绑定事务，并在请求上下文中标记事务所在服务，在服务发现的精确发现功能中把同一个请求上下文（无论在哪个节点）都转发到事务所在服务。<br/>
注意事项：

* 事务开启时需求一个 timeout，默认是10秒，当在这个时间内没有被提交或回滚，超时后会自动回滚。
* 使用分布式事务的最佳方式是采样 proxy 中的函数，而非其它自行代理操作。
* 部署的方式最好是以单独服务的方式（一个fns内只有 sql 服务）部署一个集群。
