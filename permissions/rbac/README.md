# RBAC

rbac service and enforcer for permissions.

## Install
```shell
go get github.com/aacfactory/fns-contrib/permissions/rbac
```

## Usage
Deploy service
```go
func dependencies() (v []services.Service) {
    v = []services.Service{
        // add dependencies here
		rbac.New(store),
		permissions.New(rbac.Enforcer())
    }
    return
}
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