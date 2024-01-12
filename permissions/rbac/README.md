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
Enable cache.  
Note: when enabled, then modify role will not update cache.
```yaml
permissions:
  cache:
    enable: true
    ttl: "1h0m0s"
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