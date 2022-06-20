module github.com/aacfactory/fns-contrib/permissions/store/mysql

go 1.18

replace (
	github.com/aacfactory/fns v0.11.2 => ../../../../fns
	github.com/aacfactory/fns-contrib/databases/mysql v0.11.0 => ../../../databases/mysql
)

require (
	github.com/aacfactory/errors v1.6.3
	github.com/aacfactory/fns v0.11.2
	github.com/aacfactory/fns-contrib/databases/mysql v0.11.0
	github.com/aacfactory/json v1.6.0
	github.com/aacfactory/logs v1.2.0
)