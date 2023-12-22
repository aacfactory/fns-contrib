package redis

import (
	"errors"
	"github.com/redis/rueidis"
	"time"
)

type Result interface {
	Expired() (ok bool)
	ExpireAT() (t time.Time, has bool)
	Error() (err error)
	IsNil() (ok bool)
	IsCacheHit() (ok bool)
	Message() (msg Message, err error)
	AsString() (v string, err error)
	AsBool() (v bool, err error)
	AsInt() (v int64, err error)
	AsUint() (v uint64, err error)
	AsFloat() (v float64, err error)
	AsBytes() (v []byte, err error)
	AsArray() (v []Message, err error)
	AsStrSlice() (v []string, err error)
	AsBytesSlice() (v [][]byte, err error)
	AsIntSlice() (v []int64, err error)
	AsBoolSlice() (v []bool, err error)
	AsFloatSlice() (v []float64, err error)
	AsMap() (v map[string]Message, err error)
	AsStrMap() (v map[string]string, err error)
	AsBytesMap() (v map[string][]byte, err error)
	AsBoolMap() (v map[string]bool, err error)
	AsIntMap() (v map[string]int64, err error)
	AsFloatMap() (v map[string]float64, err error)
	AsXRangeEntry() (entry XRangeEntry, err error)
	AsXRange() (entries []XRangeEntry, err error)
	AsZScore() (v ZScore, err error)
	AsZScores() (v []ZScore, err error)
	AsScanEntry() (e ScanEntry, err error)
	AsLMPop() (kvs KeyValues, err error)
	AsZMPop() (kvs KeyZScores, err error)
	AsFtSearch() (total int64, docs []FtSearchDoc, err error)
	AsFtAggregate() (total int64, docs []map[string]string, err error)
	AsFtAggregateCursor() (cursor, total int64, docs []map[string]string, err error)
	AsGeosearch() (location []GeoLocation, err error)
	AsJson(dst any) (err error)
}

func newResult(raw rueidis.RedisResult) (r result) {
	msg, msgErr := raw.ToMessage()
	if msgErr != nil {
		if rueidis.IsRedisNil(msgErr) {
			r = result{
				Msg: newMessage(msg),
				Err: "",
			}
			return
		}
		_, ok := rueidis.IsRedisErr(msgErr)
		if ok {
			r = result{
				Msg: newMessage(msg),
				Err: "",
			}
			return
		}
		r = result{
			Msg: newMessage(msg),
			Err: raw.Error().Error(),
		}
		return
	}

	r = result{
		Msg: newMessage(msg),
		Err: "",
	}
	return
}

type result struct {
	Msg message `json:"msg" avro:"msg"`
	Err string  `json:"err" avro:"err"`
}

func (r result) Expired() (ok bool) {
	if r.Err == "" {
		ok = r.Msg.Expired()
	}
	return
}

func (r result) ExpireAT() (t time.Time, has bool) {
	if r.Err == "" {
		t, has = r.Msg.ExpireAT()
	}
	return
}

func (r result) Error() (err error) {
	if r.Err == "" {
		err = r.Msg.Error()
		return
	}
	err = errors.New(r.Err)
	return
}

func (r result) IsNil() (ok bool) {
	if r.Err == "" {
		ok = r.Msg.IsNil()
	}
	return
}

func (r result) IsCacheHit() (ok bool) {
	if r.Err == "" {
		ok = r.Msg.IsCacheHit()
	}
	return
}

func (r result) Message() (msg Message, err error) {
	if r.Err == "" {
		msg = r.Msg
		return
	}
	err = errors.New(r.Err)
	return
}

func (r result) AsString() (v string, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsString()
	return
}

func (r result) AsBool() (v bool, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsBool()
	return
}

func (r result) AsInt() (v int64, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsInt()
	return
}

func (r result) AsUint() (v uint64, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsUint()
	return
}

func (r result) AsFloat() (v float64, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsFloat()
	return
}

func (r result) AsBytes() (v []byte, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsBytes()
	return
}

func (r result) AsArray() (v []Message, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsArray()
	return
}

func (r result) AsStrSlice() (v []string, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsStrSlice()
	return
}

func (r result) AsBytesSlice() (v [][]byte, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsBytesSlice()
	return
}

func (r result) AsIntSlice() (v []int64, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsIntSlice()
	return
}

func (r result) AsBoolSlice() (v []bool, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsBoolSlice()
	return
}

func (r result) AsFloatSlice() (v []float64, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsFloatSlice()
	return
}

func (r result) AsMap() (v map[string]Message, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsMap()
	return
}

func (r result) AsStrMap() (v map[string]string, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsStrMap()
	return
}

func (r result) AsBytesMap() (v map[string][]byte, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsBytesMap()
	return
}

func (r result) AsBoolMap() (v map[string]bool, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsBoolMap()
	return
}

func (r result) AsIntMap() (v map[string]int64, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsIntMap()
	return
}

func (r result) AsFloatMap() (v map[string]float64, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsFloatMap()
	return
}

func (r result) AsXRangeEntry() (entry XRangeEntry, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	entry, err = r.Msg.AsXRangeEntry()
	return
}

func (r result) AsXRange() (entries []XRangeEntry, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	entries, err = r.Msg.AsXRange()
	return
}

func (r result) AsZScore() (v ZScore, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsZScore()
	return
}

func (r result) AsZScores() (v []ZScore, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	v, err = r.Msg.AsZScores()
	return
}

func (r result) AsScanEntry() (e ScanEntry, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	e, err = r.Msg.AsScanEntry()
	return
}

func (r result) AsLMPop() (kvs KeyValues, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	kvs, err = r.Msg.AsLMPop()
	return
}

func (r result) AsZMPop() (kvs KeyZScores, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	kvs, err = r.Msg.AsZMPop()
	return
}

func (r result) AsFtSearch() (total int64, docs []FtSearchDoc, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	total, docs, err = r.Msg.AsFtSearch()
	return
}

func (r result) AsFtAggregate() (total int64, docs []map[string]string, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	total, docs, err = r.Msg.AsFtAggregate()
	return
}

func (r result) AsFtAggregateCursor() (cursor, total int64, docs []map[string]string, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	cursor, total, docs, err = r.Msg.AsFtAggregateCursor()
	return
}

func (r result) AsGeosearch() (location []GeoLocation, err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	location, err = r.Msg.AsGeosearch()
	return
}

func (r result) AsJson(dst any) (err error) {
	if r.Err != "" {
		err = errors.New(r.Err)
		return
	}
	err = r.Msg.AsJson(dst)
	return
}
