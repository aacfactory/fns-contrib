package standard

import "github.com/aacfactory/fns/service/transports"

func init() {
	transports.Register(Server())
}
