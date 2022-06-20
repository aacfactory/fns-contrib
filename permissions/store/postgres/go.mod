module github.com/aacfactory/fns-contrib/permissions/store/postgres

go 1.18

replace (
	github.com/aacfactory/fns v0.11.2 => ../../../../fns
	github.com/aacfactory/fns-contrib/databases/postgres v0.11.0 => ../../../databases/postgres
	github.com/aacfactory/fns-contrib/databases/sql v0.11.1 => ../../../databases/sql
)

require (
	github.com/aacfactory/errors v1.6.3
	github.com/aacfactory/fns v0.11.2
	github.com/aacfactory/fns-contrib/databases/postgres v0.11.0
	github.com/aacfactory/fns-contrib/databases/sql v0.11.1
	github.com/aacfactory/json v1.6.0
	github.com/aacfactory/logs v1.2.0
)