package http3

import (
	"github.com/aacfactory/fns/commons/uid"
	"github.com/quic-go/quic-go"
)

type ConnectionIDGenerator struct {
}

func (generator *ConnectionIDGenerator) GenerateConnectionID() (id quic.ConnectionID, err error) {
	id = quic.ConnectionIDFromBytes(uid.Bytes())
	return
}

func (generator *ConnectionIDGenerator) ConnectionIDLen() int {
	return 20
}
