package redis

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/json"
	rds "github.com/go-redis/redis/v8"
	"github.com/valyala/bytebufferpool"
)

type ZAddParam struct {
	Key   string          `json:"key,omitempty"`
	Score float64         `json:"score,omitempty"`
	Value json.RawMessage `json:"value,omitempty"`
}

func (svc *Service) zAdd(ctx fns.Context, param ZAddParam) (err errors.CodeError) {
	cmdErr := svc.client.Writer().ZAdd(ctx, param.Key, &rds.Z{
		Score:  param.Score,
		Member: param.Value,
	}).Err()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}
	return
}

func (svc *Service) zCard(ctx fns.Context, key string) (num int64, err errors.CodeError) {
	n, cmdErr := svc.client.Reader().ZCard(ctx, key).Result()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}
	num = n
	return
}

type ZRangeParam struct {
	Key   string `json:"key,omitempty"`
	Start int64  `json:"start,omitempty"`
	Stop  int64  `json:"stop,omitempty"`
}

func (svc *Service) zRange(ctx fns.Context, param ZRangeParam) (result *json.Array, err errors.CodeError) {
	n, cmdErr := svc.client.Reader().ZRange(ctx, param.Key, param.Start, param.Stop).Result()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}

	if n == nil || len(n) == 0 {
		result = json.NewArray()
		return
	}

	p := bytebufferpool.Get()
	defer bytebufferpool.Put(p)
	_ = p.WriteByte('[')
	for i, s := range n {
		if i == 0 {
			_, _ = p.WriteString(s)
		} else {
			_ = p.WriteByte(',')
			_, _ = p.WriteString(s)
		}
	}
	result = json.NewArrayFromBytes(p.Bytes())
	return
}

type ZRangeByScoreParam struct {
	Key    string `json:"key,omitempty"`
	Min    string `json:"min,omitempty"`
	Max    string `json:"max,omitempty"`
	Offset int64  `json:"offset,omitempty"`
	Count  int64  `json:"count,omitempty"`
}

func (svc *Service) zRangeByScore(ctx fns.Context, param ZRangeByScoreParam) (result *json.Array, err errors.CodeError) {
	n, cmdErr := svc.client.Reader().ZRangeByScore(ctx, param.Key, &rds.ZRangeBy{
		Min:    param.Min,
		Max:    param.Max,
		Offset: param.Offset,
		Count:  param.Count,
	}).Result()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}

	if n == nil || len(n) == 0 {
		result = json.NewArray()
		return
	}

	p := bytebufferpool.Get()
	defer bytebufferpool.Put(p)
	_ = p.WriteByte('[')
	for i, s := range n {
		if i == 0 {
			_, _ = p.WriteString(s)
		} else {
			_ = p.WriteByte(',')
			_, _ = p.WriteString(s)
		}
	}
	result = json.NewArrayFromBytes(p.Bytes())
	return
}

type ZRemParam struct {
	Key    string          `json:"key,omitempty"`
	Member json.RawMessage `json:"min,omitempty"`
}

func (svc *Service) zRem(ctx fns.Context, param ZRemParam) (ok bool, err errors.CodeError) {
	n, cmdErr := svc.client.Writer().ZRem(ctx, param.Key, param.Member).Result()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}

	ok = n > 0
	return
}

type ZRemByRankParam struct {
	Key   string `json:"key,omitempty"`
	Start int64  `json:"start,omitempty"`
	Stop  int64  `json:"stop,omitempty"`
}

func (svc *Service) zRemByRank(ctx fns.Context, param ZRemByRankParam) (ok bool, err errors.CodeError) {
	n, cmdErr := svc.client.Writer().ZRemRangeByRank(ctx, param.Key, param.Start, param.Stop).Result()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}

	ok = n > 0
	return
}

type ZRemByScoreParam struct {
	Key string `json:"key,omitempty"`
	Min string `json:"min,omitempty"`
	Max string `json:"max,omitempty"`
}

func (svc *Service) zRemByScore(ctx fns.Context, param ZRemByScoreParam) (ok bool, err errors.CodeError) {
	n, cmdErr := svc.client.Writer().ZRemRangeByScore(ctx, param.Key, param.Min, param.Max).Result()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}

	ok = n > 0
	return
}

type ZRevRangeParam struct {
	Key   string `json:"key,omitempty"`
	Start int64  `json:"start,omitempty"`
	Stop  int64  `json:"stop,omitempty"`
}

func (svc *Service) zRevRange(ctx fns.Context, param ZRevRangeParam) (result *json.Array, err errors.CodeError) {
	n, cmdErr := svc.client.Reader().ZRevRange(ctx, param.Key, param.Start, param.Stop).Result()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}

	if n == nil || len(n) == 0 {
		result = json.NewArray()
		return
	}

	p := bytebufferpool.Get()
	defer bytebufferpool.Put(p)
	_ = p.WriteByte('[')
	for i, s := range n {
		if i == 0 {
			_, _ = p.WriteString(s)
		} else {
			_ = p.WriteByte(',')
			_, _ = p.WriteString(s)
		}
	}
	result = json.NewArrayFromBytes(p.Bytes())
	return
}

type ZRevRangeByScoreParam struct {
	Key    string `json:"key,omitempty"`
	Min    string `json:"min,omitempty"`
	Max    string `json:"max,omitempty"`
	Offset int64  `json:"offset,omitempty"`
	Count  int64  `json:"count,omitempty"`
}

func (svc *Service) zRevRangeByScore(ctx fns.Context, param ZRevRangeByScoreParam) (result *json.Array, err errors.CodeError) {
	n, cmdErr := svc.client.Reader().ZRevRangeByScore(ctx, param.Key, &rds.ZRangeBy{
		Min:    param.Min,
		Max:    param.Max,
		Offset: param.Offset,
		Count:  param.Count,
	}).Result()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}

	if n == nil || len(n) == 0 {
		result = json.NewArray()
		return
	}

	p := bytebufferpool.Get()
	defer bytebufferpool.Put(p)
	_ = p.WriteByte('[')
	for i, s := range n {
		if i == 0 {
			_, _ = p.WriteString(s)
		} else {
			_ = p.WriteByte(',')
			_, _ = p.WriteString(s)
		}
	}
	result = json.NewArrayFromBytes(p.Bytes())
	return
}
