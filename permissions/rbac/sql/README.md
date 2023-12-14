# SQL

sql based rbac store

## Install
```shell
go get github.com/aacfactory/fns-contrib/permissions/rbac/sql
```

## Usage
```go
app.Deploy(rbac.New(sql.Store()))
```

## Config 
```yaml
rbac:
  sql:
    endpoint: "endpoint name of sql service"
    roleTable: 
      schema: "schema"
      table: "table"
    userTable:
      schema: "schema"
      table: "table"
    cache:
      disable: false
      rolesTTL: "24h0m0s"
      userRolesTTL: "1h0m0s"
```

## Table schema
| Column      | Type        | Not null | Remark |
|-------------|-------------|----------|--------|
| ID          | string      | yes      | pk     |
| CREATE_BY   | string      | yes      |        |
| CREATE_AT   | time        | yes      | UTC    |
| MODIFY_BY   | string      | no       |        |
| MODIFY_AT   | time        | no       | UTC    |
| VERSION     | int64       | yes      |        |
| NAME        | string      | yes      | unique |
| DESCRIPTION | string      | no       |        |
| PARENT_ID   | string      | no       |        |
| POLICIES    | json bytes  | yes      | array  | 

