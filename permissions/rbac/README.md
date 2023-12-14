# RBAC

rbac service and enforcer for permissions.

## Install
```shell
go get github.com/aacfactory/fns-contrib/permissions/rbac
```

## Usage
Deploy service
```go
app.Deploy(rbac.New(store))
app.Deploy(permissions.New(rbac.Enforcer()))
```

## Store
Implement rbac.Store or use [sql](https://github.com/aacfactory/fns-contrib/tree/main/permissions/rbac/sql).

## Functions
* Bind
* Unbind
* Bounds
* Get role
* List roles
* Save role
* Remove role