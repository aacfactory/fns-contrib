module github.com/aacfactory/fns-contrib/databases/mysql

go 1.18

replace (
	github.com/aacfactory/fns v0.11.2 => ../../../fns
	github.com/aacfactory/fns-contrib/databases/sql v0.11.1 => ../sql
)

require (
	github.com/aacfactory/errors v1.6.3
	github.com/aacfactory/fns v0.11.2
	github.com/aacfactory/fns-contrib/databases/sql v0.11.1
	github.com/aacfactory/json v1.6.0
	golang.org/x/sync v0.0.0-20220601150217-0de741cfad7f
)

require (
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/rs/xid v1.4.0 // indirect
	github.com/tidwall/gjson v1.14.1 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tidwall/sjson v1.2.4 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
)
