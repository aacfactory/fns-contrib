module github.com/aacfactory/fns-contrib/permissions/rbac/redis

go 1.20

replace (
	github.com/aacfactory/fns-contrib/permissions/rbac v0.0.0 => ../../rbac
	github.com/aacfactory/fns-contrib/databases/redis v1.0.1 => ../../../databases/redis
)

require (
	github.com/aacfactory/fns v1.0.5
	github.com/aacfactory/fns-contrib/permissions/rbac v0.0.0
	github.com/aacfactory/logs v1.13.0
	github.com/aacfactory/fns-contrib/databases/redis v1.0.1
)
