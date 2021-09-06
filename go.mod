module github.com/aacfactory/fns-contrib

replace (
	authorizations/jwt latest  => ./authorizations/jwt
)

require (
	./authorizations/jwt latest
)

go 1.17
