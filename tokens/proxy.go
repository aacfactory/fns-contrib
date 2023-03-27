package tokens

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
)

func Get(ctx context.Context, id string) (token Token, err errors.CodeError) {
	if id == "" {
		err = errors.Warning("tokens: get failed").WithCause(errors.Warning("id is required"))
		return
	}
	endpoint, hasEndpoint := service.GetEndpoint(ctx, name)
	if !hasEndpoint {
		err = errors.Warning("tokens: get failed").WithCause(errors.Warning("tokens: service was not deployed"))
		return
	}
	future, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, getFn, service.NewArgument(id), service.WithInternalRequest()))
	if requestErr != nil {
		err = requestErr
		return
	}
	scanErr := future.Scan(&token)
	if scanErr != nil {
		err = errors.Warning("tokens: get failed").WithCause(scanErr)
		return
	}
	return
}

func List(ctx context.Context, userId string) (tokens []Token, err errors.CodeError) {
	if userId == "" {
		err = errors.Warning("tokens: list failed").WithCause(errors.Warning("user id is required"))
		return
	}
	endpoint, hasEndpoint := service.GetEndpoint(ctx, name)
	if !hasEndpoint {
		err = errors.Warning("tokens: list failed").WithCause(errors.Warning("tokens: service was not deployed"))
		return
	}
	future, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, listFn, service.NewArgument(userId), service.WithInternalRequest()))
	if requestErr != nil {
		err = requestErr
		return
	}
	tokens = make([]Token, 0, 1)
	scanErr := future.Scan(&tokens)
	if scanErr != nil {
		err = errors.Warning("tokens: list failed").WithCause(scanErr)
		return
	}
	return
}

func Save(ctx context.Context, param SaveParam) (err errors.CodeError) {
	if param.Id == "" {
		err = errors.Warning("tokens: save failed").WithCause(errors.Warning("id is required"))
		return
	}
	if param.UserId == "" {
		err = errors.Warning("tokens: save failed").WithCause(errors.Warning("user id is required"))
		return
	}
	if param.Token == "" {
		err = errors.Warning("tokens: save failed").WithCause(errors.Warning("token is required"))
		return
	}
	endpoint, hasEndpoint := service.GetEndpoint(ctx, name)
	if !hasEndpoint {
		err = errors.Warning("tokens: save failed").WithCause(errors.Warning("tokens: service was not deployed"))
		return
	}
	_, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, saveFn, service.NewArgument(param), service.WithInternalRequest()))
	if requestErr != nil {
		err = requestErr
		return
	}
	return
}

func Remove(ctx context.Context, param RemoveParam) (err errors.CodeError) {
	if param.Id == "" && param.UserId == "" {
		err = errors.Warning("tokens: remove failed").WithCause(errors.Warning("one of id or user id is required"))
		return
	}
	endpoint, hasEndpoint := service.GetEndpoint(ctx, name)
	if !hasEndpoint {
		err = errors.Warning("tokens: remove failed").WithCause(errors.Warning("tokens: service was not deployed"))
		return
	}
	_, requestErr := endpoint.RequestSync(ctx, service.NewRequest(ctx, name, removeFn, service.NewArgument(param), service.WithInternalRequest()))
	if requestErr != nil {
		err = requestErr
		return
	}
	return
}
