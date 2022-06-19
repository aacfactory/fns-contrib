package internal

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
)

type Database struct {
	log    logs.Logger
	client Client
}

func (db *Database) HandleCommand(ctx context.Context, name string, params []interface{}) (result []byte, err errors.CodeError) {
	switch name {
	case KEYS:
		v, handleErr := keys(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case DEL:
		handleErr := del(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		return
	case EXISTS:
		v, handleErr := exists(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case EXPIRE:
		handleErr := expire(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		return
	case PERSIST:
		handleErr := persist(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		return
	case SCAN:
		v, next, handleErr := scan(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(map[string]interface{}{"keys": v, "next": next})
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case SET:
		handleErr := set(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		return
	case SETNX:
		handleErr := setNX(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		return
	case GET:
		v, handleErr := get(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case GETSET:
		v, handleErr := getSet(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case MGET:
		v, handleErr := mget(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case MSET:
		handleErr := mset(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		return
	case SETEX:
		handleErr := setEX(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		return
	case INCR:
		v, handleErr := incr(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case INCRBY:
		v, handleErr := incrBy(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case DECR:
		v, handleErr := decr(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case DECRBY:
		v, handleErr := decrBy(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case APPEND:
		v, handleErr := append0(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case HDEL:
		handleErr := hdel(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		return
	case HGET:
		v, handleErr := hget(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case HEXISTS:
		v, handleErr := hexist(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case HGETALL:
		v, handleErr := hgetall(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case HINCERBY:
		v, handleErr := hincrby(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case HKEYS:
		v, handleErr := hkeys(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case HLEN:
		v, handleErr := hlen(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case HMGET:
		v, handleErr := hmget(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case HMSET:
		v, handleErr := hmset(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case HSET:
		handleErr := hset(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		return
	case HSETNX:
		v, handleErr := hsetnx(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case HVALS:
		v, handleErr := hvals(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case HSCAN:
		v, next, handleErr := hscan(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(map[string]interface{}{"keys": v, "next": next})
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case SORT:
		v, handleErr := sort(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case SADD:
		v, handleErr := sadd(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case SMEMBERS:
		v, handleErr := smembers(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	case SREM:
		v, handleErr := srem(ctx, db.client, params)
		if handleErr != nil {
			err = handleErr
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
		// todo list set sorted-set
	default:
		args := make([]interface{}, 0, 1)
		args = append(args, name)
		args = append(args, params...)
		v, handleErr := db.client.Do(ctx, args).Result()
		if handleErr != nil {
			err = errors.ServiceError("redis: handle command failed").WithCause(handleErr).WithMeta("command", name)
			return
		}
		p, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			err = errors.ServiceError("redis: encode result failed").WithCause(encodeErr)
			return
		}
		result = p
		return
	}
}

func (db *Database) Close() {
	err := db.client.Close()
	if db.log.DebugEnabled() {
		if err == nil {
			db.log.Debug().Caller().Message("redis: close")
		} else {
			db.log.Debug().Caller().Cause(err).Message("redis: close failed")
		}
	}
}
