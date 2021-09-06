module github.com/aacfactory/fns-contrib

go 1.17

replace (
	github.com/aacfactory/fns-contrib/authorizations/jwt => ./authorizations/jwt
	github.com/aacfactory/fns-contrib/databases/sql => ./databases/sql
)
