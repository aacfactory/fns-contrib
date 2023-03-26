package rbac

import "github.com/aacfactory/fns/service"

type BindArgument struct {
	UserId string   `json:"userId"`
	Roles  []string `json:"roles"`
}

type Store interface {
	service.Component
}
