package websockets

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/json"
	"strings"
)

type sendParam struct {
	ConnectionId string          `json:"connectionId"`
	Payload      json.RawMessage `json:"payload"`
}

func Send(ctx context.Context, connectionId string, payload interface{}) (err error) {
	connectionId = strings.TrimSpace(connectionId)
	if connectionId == "" {
		err = errors.BadRequest("websockets: connectionId is required")
		return
	}
	if payload == nil {
		err = errors.BadRequest("websockets: payload is required")
		return
	}
	p, encodeErr := json.Marshal(payload)
	if encodeErr != nil {
		err = errors.BadRequest("websockets: payload is invalid").WithCause(encodeErr).WithMeta("connection", connectionId)
		return
	}
	store := service.SharedStore(ctx)
	hostId, has, getErr := store.Get(ctx, bytex.FromString(fmt.Sprintf("fns/websockets/%s", connectionId)))
	if getErr != nil {
		err = errors.Warning("websockets: get host of connection failed").WithCause(encodeErr).WithMeta("connection", connectionId)
		return
	}
	if !has {
		err = errors.Warning("websockets: host of connection was not found").WithCause(encodeErr).WithMeta("connection", connectionId)
		return
	}
	endpoint, hasEndpoint := service.GetEndpoint(ctx, handleName, service.Exact(bytex.ToString(hostId)))
	if !hasEndpoint {
		err = errors.Warning("websockets: endpoint of connection was not found").
			WithMeta("service", handleName).WithMeta("connection", connectionId).WithMeta("hostId", bytex.ToString(hostId))
		return
	}
	fr := endpoint.Request(
		ctx,
		service.NewRequest(
			ctx,
			handleName, sendFn,
			service.NewArgument(&sendParam{
				ConnectionId: connectionId,
				Payload:      p,
			}),
			service.WithInternalRequest(),
		),
	)
	_, err = fr.Get(ctx)
	return
}
