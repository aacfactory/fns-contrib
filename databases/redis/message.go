package redis

import (
	"errors"
	"fmt"
	"github.com/aacfactory/json"
	"github.com/redis/rueidis"
	"math"
	"strconv"
	"time"
	"unsafe"
)

type XRangeEntry struct {
	FieldValues map[string]string `json:"fieldValues"`
	Id          string            `json:"id"`
}

type ZScore struct {
	Member string  `json:"member"`
	Score  float64 `json:"score"`
}

type ScanEntry struct {
	Elements []string `json:"elements"`
	Cursor   uint64   `json:"cursor"`
}

type KeyValues struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

type KeyZScores struct {
	Key    string   `json:"key"`
	Values []ZScore `json:"values"`
}

type FtSearchDoc struct {
	Doc map[string]string `json:"doc"`
	Key string            `json:"key"`
}

type GeoLocation struct {
	Name      string  `json:"name"`
	Longitude float64 `json:"longitude"`
	Latitude  float64 `json:"latitude"`
	Dist      float64 `json:"dist"`
	GeoHash   int64   `json:"geoHash"`
}

type Message interface {
	Expired() (ok bool)
	ExpireAT() (t time.Time, has bool)
	Error() (err error)
	IsNil() (ok bool)
	IsString() (ok bool)
	IsBool() (ok bool)
	IsInt() (ok bool)
	IsFloat() (ok bool)
	IsArray() (ok bool)
	IsMap() (ok bool)
	IsCacheHit() (ok bool)
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

const (
	typeError int = iota + 1
	typeString
	typeBool
	typeInt
	typeFloat
	typeArray
	typeMap
)

const (
	ErrNil         = "REDIS: NIL"
	ErrMoved       = "REDIS: MOVED"
	ErrAsk         = "REDIS: ASK"
	ErrTryAgain    = "REDIS: TRY_AGAIN"
	ErrClusterDown = "REDIS: CLUSTER_DOWN"
	ErrNoScript    = "REDIS: NO_SCRIPT"
)

func newMessage(raw rueidis.RedisMessage) (v message) {
	if err := raw.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			v = message{
				Type:     typeError,
				Content:  ErrNil,
				Values:   nil,
				Deadline: "",
				CacheHit: raw.IsCacheHit(),
			}
			return
		}
		if rErr, ok := rueidis.IsRedisErr(err); ok {
			if rErr.IsNil() {
				v = message{
					Type:     typeError,
					Content:  ErrNil,
					Values:   nil,
					Deadline: "",
					CacheHit: raw.IsCacheHit(),
				}
				return
			}
			if rErr.IsNoScript() {
				v = message{
					Type:     typeError,
					Content:  ErrNoScript,
					Values:   nil,
					Deadline: "",
					CacheHit: raw.IsCacheHit(),
				}
				return
			}
			if rErr.IsTryAgain() {
				v = message{
					Type:     typeError,
					Content:  ErrTryAgain,
					Values:   nil,
					Deadline: "",
					CacheHit: raw.IsCacheHit(),
				}
				return
			}
			if rErr.IsClusterDown() {
				v = message{
					Type:     typeError,
					Content:  ErrClusterDown,
					Values:   nil,
					Deadline: "",
					CacheHit: raw.IsCacheHit(),
				}
				return
			}
			if addr, isAsk := rErr.IsAsk(); isAsk {
				v = message{
					Type:     typeError,
					Content:  ErrAsk,
					Values:   []message{{Content: addr}},
					Deadline: "",
					CacheHit: raw.IsCacheHit(),
				}
				return
			}
			if addr, isAsk := rErr.IsMoved(); isAsk {
				v = message{
					Type:     typeError,
					Content:  ErrMoved,
					Values:   []message{{Content: addr}},
					Deadline: "",
					CacheHit: raw.IsCacheHit(),
				}
				return
			}
		}
		v = message{
			Type:     typeError,
			Content:  err.Error(),
			Values:   nil,
			Deadline: "",
			CacheHit: raw.IsCacheHit(),
		}
		return
	}
	deadline := ""
	if expireAT := raw.CachePXAT(); expireAT > 0 {
		deadline = strconv.FormatInt(expireAT, 10)
	}
	if raw.IsString() {
		vv, _ := raw.ToString()
		v = message{
			Type:     typeString,
			Content:  vv,
			Values:   nil,
			Deadline: deadline,
			CacheHit: raw.IsCacheHit(),
		}
		return
	}
	if raw.IsBool() {
		vv, _ := raw.AsBool()
		v = message{
			Type:     typeBool,
			Content:  strconv.FormatBool(vv),
			Values:   nil,
			Deadline: deadline,
			CacheHit: raw.IsCacheHit(),
		}
		return
	}
	if raw.IsInt64() {
		vv, _ := raw.AsInt64()
		v = message{
			Type:     typeInt,
			Content:  strconv.FormatInt(vv, 10),
			Values:   nil,
			Deadline: deadline,
			CacheHit: raw.IsCacheHit(),
		}
		return
	}
	if raw.IsFloat64() {
		vv, _ := raw.AsFloat64()
		v = message{
			Type:     typeFloat,
			Content:  strconv.FormatFloat(vv, 'f', 6, 64),
			Values:   nil,
			Deadline: deadline,
			CacheHit: raw.IsCacheHit(),
		}
		return
	}
	if raw.IsArray() {
		vv, _ := raw.ToArray()
		v = message{
			Type:     typeArray,
			Content:  "",
			Values:   make([]message, 0, len(vv)),
			Deadline: deadline,
			CacheHit: raw.IsCacheHit(),
		}
		for _, msg := range vv {
			v.Values = append(v.Values, newMessage(msg))
		}
		return
	}
	if raw.IsMap() {
		vv, _ := raw.AsMap()
		v = message{
			Type:     typeMap,
			Content:  "",
			Values:   make([]message, 0, len(vv)),
			Deadline: deadline,
			CacheHit: raw.IsCacheHit(),
		}
		for key, msg := range vv {
			element := newMessage(msg)
			element.Content = key
			v.Values = append(v.Values, element)
		}
	}
	return
}

type message struct {
	Type     int       `json:"type"`
	Content  string    `json:"content"`
	Values   []message `json:"values"`
	Deadline string    `json:"deadline"`
	CacheHit bool      `json:"cacheHit"`
}

func (m message) Expired() (ok bool) {
	if m.Error() != nil {
		return
	}
	if m.Deadline == "" {
		return
	}
	deadline, _ := strconv.ParseInt(m.Deadline, 10, 64)
	if deadline == 0 {
		return
	}
	ok = deadline-time.Now().UnixMilli() < 0
	return
}

func (m message) ExpireAT() (t time.Time, has bool) {
	if m.Error() != nil {
		return
	}
	if m.Deadline == "" {
		return
	}
	deadline, _ := strconv.ParseInt(m.Deadline, 10, 64)
	if deadline == 0 {
		return
	}
	t = time.UnixMilli(deadline)
	has = true
	return
}

func (m message) Error() (err error) {
	if m.Type == typeError {
		err = &Error{
			m,
		}
		return
	}
	return
}

func (m message) IsNil() (ok bool) {
	if m.Type == typeError {
		ok = m.Content == ErrNil
		return
	}
	return
}

func (m message) IsString() (ok bool) {
	ok = m.Type == typeString
	return
}

func (m message) IsBool() (ok bool) {
	ok = m.Type == typeBool
	return
}

func (m message) IsInt() (ok bool) {
	ok = m.Type == typeInt
	return
}

func (m message) IsFloat() (ok bool) {
	ok = m.Type == typeFloat
	return
}

func (m message) IsArray() (ok bool) {
	ok = m.Type == typeArray
	return
}

func (m message) IsMap() (ok bool) {
	ok = m.Type == typeMap
	return
}

func (m message) IsCacheHit() (ok bool) {
	ok = m.CacheHit
	return
}

func (m message) AsString() (v string, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeString:
		v = m.Content
		break
	case typeBool:
		v = m.Content
		break
	case typeInt:
		v = m.Content
		break
	case typeFloat:
		v = m.Content
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS STRING")
		return
	}
	return
}

func (m message) AsBool() (v bool, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeString:
		v = m.Content == "true"
		break
	case typeBool:
		v = m.Content == "true"
		break
	case typeInt:
		v = m.Content == "1"
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS BOOL")
		return
	}
	return
}

func (m message) AsInt() (v int64, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeString:
		v, err = strconv.ParseInt(m.Content, 10, 64)
		break
	case typeInt:
		v, err = strconv.ParseInt(m.Content, 10, 64)
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS INT")
		return
	}
	return
}

func (m message) AsUint() (v uint64, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeString:
		v, err = strconv.ParseUint(m.Content, 10, 64)
		break
	case typeInt:
		v, err = strconv.ParseUint(m.Content, 10, 64)
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS UINT")
		return
	}
	return
}

func (m message) AsFloat() (v float64, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeString:
		v, err = strconv.ParseFloat(m.Content, 64)
		if err != nil && m.Content == "-nan" {
			return math.NaN(), nil
		}
		break
	case typeInt:
		v, err = strconv.ParseFloat(m.Content, 64)
		if err != nil && m.Content == "-nan" {
			return math.NaN(), nil
		}
		break
	case typeFloat:
		v, err = strconv.ParseFloat(m.Content, 64)
		if err != nil && m.Content == "-nan" {
			return math.NaN(), nil
		}
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS FLOAT")
		return
	}
	return
}

func (m message) AsBytes() (v []byte, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeString:
		v = unsafe.Slice(unsafe.StringData(m.Content), len(m.Content))
		break
	case typeBool:
		v = unsafe.Slice(unsafe.StringData(m.Content), len(m.Content))
		break
	case typeInt:
		v = unsafe.Slice(unsafe.StringData(m.Content), len(m.Content))
		break
	case typeFloat:
		v = unsafe.Slice(unsafe.StringData(m.Content), len(m.Content))
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS BYTES")
		return
	}
	return
}

func (m message) AsArray() (v []Message, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeArray:
		for _, value := range m.Values {
			v = append(v, value)
		}
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS ARRAY")
		return
	}
	return
}

func (m message) AsStrSlice() (v []string, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeArray:
		for _, value := range m.Values {
			element, elementErr := value.AsString()
			if elementErr != nil {
				err = elementErr
				return
			}
			v = append(v, element)
		}
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS ARRAY")
		return
	}
	return
}

func (m message) AsBytesSlice() (v [][]byte, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeArray:
		for _, value := range m.Values {
			element, elementErr := value.AsBytes()
			if elementErr != nil {
				err = elementErr
				return
			}
			v = append(v, element)
		}
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS ARRAY")
		return
	}
	return
}

func (m message) AsIntSlice() (v []int64, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeArray:
		for _, value := range m.Values {
			element, elementErr := value.AsInt()
			if elementErr != nil {
				err = elementErr
				return
			}
			v = append(v, element)
		}
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS ARRAY")
		return
	}
	return
}

func (m message) AsBoolSlice() (v []bool, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeArray:
		for _, value := range m.Values {
			element, elementErr := value.AsBool()
			if elementErr != nil {
				err = elementErr
				return
			}
			v = append(v, element)
		}
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS ARRAY")
		return
	}
	return
}

func (m message) AsFloatSlice() (v []float64, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeArray:
		for _, value := range m.Values {
			element, elementErr := value.AsFloat()
			if elementErr != nil {
				err = elementErr
				return
			}
			v = append(v, element)
		}
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS ARRAY")
		return
	}
	return
}

func (m message) AsMap() (v map[string]Message, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeMap:
		for _, value := range m.Values {
			v[value.Content] = value
		}
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS MAP")
		return
	}
	return
}

func (m message) AsStrMap() (v map[string]string, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeMap:
		for _, value := range m.Values {
			element, elementErr := value.AsString()
			if elementErr != nil {
				err = elementErr
				return
			}
			v[value.Content] = element
		}
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS MAP")
		return
	}
	return
}

func (m message) AsBytesMap() (v map[string][]byte, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeMap:
		for _, value := range m.Values {
			element, elementErr := value.AsBytes()
			if elementErr != nil {
				err = elementErr
				return
			}
			v[value.Content] = element
		}
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS MAP")
		return
	}
	return
}

func (m message) AsBoolMap() (v map[string]bool, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeMap:
		for _, value := range m.Values {
			element, elementErr := value.AsBool()
			if elementErr != nil {
				err = elementErr
				return
			}
			v[value.Content] = element
		}
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS MAP")
		return
	}
	return
}

func (m message) AsIntMap() (v map[string]int64, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeMap:
		for _, value := range m.Values {
			element, elementErr := value.AsInt()
			if elementErr != nil {
				err = elementErr
				return
			}
			v[value.Content] = element
		}
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS MAP")
		return
	}
	return
}

func (m message) AsFloatMap() (v map[string]float64, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeMap:
		for _, value := range m.Values {
			element, elementErr := value.AsFloat()
			if elementErr != nil {
				err = elementErr
				return
			}
			v[value.Content] = element
		}
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS MAP")
		return
	}
	return
}

func (m message) AsXRangeEntry() (entry XRangeEntry, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeArray:
		if len(m.Values) != 2 {
			err = errors.New("REDIS: VALUE CAN NOT AS XRangeEntry")
			return
		}
		if m.Values[0].Type != typeString {
			err = errors.New("REDIS: VALUE CAN NOT AS XRangeEntry")
			return
		}
		id, idErr := m.Values[0].AsString()
		if idErr != nil {
			err = idErr
			return
		}
		if m.Values[1].Type != typeMap {
			err = errors.New("REDIS: VALUE CAN NOT AS XRangeEntry")
			return
		}
		fields, fieldsErr := m.Values[1].AsStrMap()
		if fieldsErr != nil {
			err = fieldsErr
			return
		}
		entry.Id = id
		entry.FieldValues = fields
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS XRangeEntry")
		return
	}
	return
}

func (m message) AsXRange() (entries []XRangeEntry, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeArray:
		for _, value := range m.Values {
			entry, entryErr := value.AsXRangeEntry()
			if entryErr != nil {
				err = entryErr
				return
			}
			entries = append(entries, entry)
		}
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS XRange")
		return
	}
	return
}

func (m message) AsZScore() (v ZScore, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeArray:
		if len(m.Values) == 2 {
			if v.Member, err = m.Values[0].AsString(); err == nil {
				v.Score, err = m.Values[1].AsFloat()
			}
			return v, err
		}
		err = errors.New("REDIS: VALUE CAN NOT AS ZScore")
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS ZScore")
		return
	}
	return
}

func (m message) AsZScores() (v []ZScore, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeArray:
		arr := m.Values
		if len(arr) > 0 && arr[0].Type == typeArray {
			for _, value := range arr {
				element, elementErr := value.AsZScore()
				if elementErr != nil {
					err = elementErr
					return
				}
				v = append(v, element)
			}
			return
		}
		v = make([]ZScore, len(arr)/2)
		for i := 0; i < len(v); i++ {
			j := i * 2
			value := arr[j : j+2]
			if len(value) == 2 {
				member, memberErr := value[0].AsString()
				if memberErr != nil {
					err = memberErr
					return
				}
				score, scoreErr := value[1].AsFloat()
				if scoreErr != nil {
					err = scoreErr
					return
				}
				v[i] = ZScore{
					Member: member,
					Score:  score,
				}
			}
		}
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS ZScores")
		return
	}
	return
}

func (m message) AsScanEntry() (e ScanEntry, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeArray:
		if len(m.Values) >= 2 {
			if e.Cursor, err = m.Values[0].AsUint(); err != nil {
				return
			}
			if e.Elements, err = m.Values[1].AsStrSlice(); err != nil {
				return
			}
		}
		err = errors.New("REDIS: VALUE CAN NOT AS ScanEntry")
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS ScanEntry")
		return
	}
	return
}

func (m message) AsLMPop() (kvs KeyValues, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeArray:
		if len(m.Values) >= 2 {
			if kvs.Key, err = m.Values[0].AsString(); err != nil {
				return
			}
			if kvs.Values, err = m.Values[1].AsStrSlice(); err != nil {
				return
			}
		}
		err = errors.New("REDIS: VALUE CAN NOT AS LMPop")
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS LMPop")
		return
	}
	return
}

func (m message) AsZMPop() (kvs KeyZScores, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeArray:
		if len(m.Values) >= 2 {
			if kvs.Key, err = m.Values[0].AsString(); err != nil {
				return
			}
			if kvs.Values, err = m.Values[1].AsZScores(); err != nil {
				return
			}
		}
		err = errors.New("REDIS: VALUE CAN NOT AS ZMPop")
		break
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS ZMPop")
		return
	}
	return
}

func (m message) AsFtSearch() (total int64, docs []FtSearchDoc, err error) {
	if err = m.Error(); err != nil {
		return
	}
	if m.IsMap() {
		for i := 0; i < len(m.Values); i += 2 {
			switch m.Values[i].Content {
			case "total_results":
				total, err = m.Values[i+1].AsInt()
				if err != nil {
					return
				}
				break
			case "results":
				records := m.Values[i+1].Values
				docs = make([]FtSearchDoc, len(records))
				for d, record := range records {
					for j := 0; j < len(record.Values); j += 2 {
						switch record.Values[j].Content {
						case "id":
							docs[d].Key = record.Values[j+1].Content
						case "extra_attributes":
							docs[d].Doc, _ = record.Values[j+1].AsStrMap()
						}
					}
				}
				break
			case "error":
				for _, e := range m.Values[i+1].Values {
					return 0, nil, &Error{
						message: e,
					}
				}
			}
		}
		return
	}
	if len(m.Values) > 0 {
		total, err = m.Values[0].AsInt()
		if err != nil {
			return
		}
		if len(m.Values) > 2 && m.Values[2].Content == "" {
			docs = make([]FtSearchDoc, 0, (len(m.Values)-1)/2)
			for i := 1; i < len(m.Values); i += 2 {
				doc, _ := m.Values[i+1].AsStrMap()
				docs = append(docs, FtSearchDoc{Doc: doc, Key: m.Values[i].Content})
			}
		} else {
			docs = make([]FtSearchDoc, 0, len(m.Values)-1)
			for i := 1; i < len(m.Values); i++ {
				docs = append(docs, FtSearchDoc{Doc: nil, Key: m.Values[i].Content})
			}
		}
		return
	}
	err = errors.New("REDIS: VALUE CAN NOT AS FtSearch")
	return
}

func (m message) AsFtAggregate() (total int64, docs []map[string]string, err error) {
	if err = m.Error(); err != nil {
		return
	}
	if m.IsMap() {
		for i := 0; i < len(m.Values); i += 2 {
			switch m.Values[i].Content {
			case "total_results":
				total, err = m.Values[i+1].AsInt()
				if err != nil {
					return
				}
			case "results":
				records := m.Values[i+1].Values
				docs = make([]map[string]string, len(records))
				for d, record := range records {
					for j := 0; j < len(record.Values); j += 2 {
						switch record.Values[j].Content {
						case "extra_attributes":
							docs[d], _ = record.Values[j+1].AsStrMap()
						}
					}
				}
			case "error":
				for _, e := range m.Values[i+1].Values {
					return 0, nil, &Error{
						message: e,
					}
				}
			}
		}
		return
	}
	if len(m.Values) > 0 {
		total, err = m.Values[0].AsInt()
		if err != nil {
			return
		}
		docs = make([]map[string]string, len(m.Values)-1)
		for d, record := range m.Values[1:] {
			docs[d], _ = record.AsStrMap()
		}
		return
	}
	err = errors.New("REDIS: VALUE CAN NOT AS FtAggregate")
	return
}

func (m message) AsFtAggregateCursor() (cursor, total int64, docs []map[string]string, err error) {
	if err = m.Error(); err != nil {
		return
	}
	if m.IsArray() && len(m.Values) == 2 && (m.Values[0].IsArray() || m.Values[0].IsMap()) {
		total, docs, err = m.Values[0].AsFtAggregate()
		cursor, err = m.Values[1].AsInt()
	} else {
		total, docs, err = m.AsFtAggregate()
	}
	return
}

func (m message) AsGeosearch() (location []GeoLocation, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.Type {
	case typeArray:
		arr := m.Values
		location = make([]GeoLocation, 0, len(arr))
		for _, v := range arr {
			var loc GeoLocation
			if v.IsString() {
				loc.Name = v.Content
			} else {
				info := v.Values
				var i int

				//name
				loc.Name = info[i].Content
				i++
				//distance
				if i < len(info) && info[i].Content != "" {
					loc.Dist, err = info[i].AsFloat()
					if err != nil {
						return nil, err
					}
					i++
				}
				//hash
				if i < len(info) && info[i].IsInt() {
					loc.GeoHash, _ = info[i].AsInt()
					i++
				}
				//coordinates
				if i < len(info) && info[i].Values != nil {
					cord := info[i].Values
					if len(cord) < 2 {
						return nil, fmt.Errorf("got %d, expected 2", len(info))
					}
					loc.Longitude, _ = cord[0].AsFloat()
					loc.Latitude, _ = cord[1].AsFloat()
				}
			}
			location = append(location, loc)
		}
	default:
		err = errors.New("REDIS: VALUE CAN NOT AS Geosearch")
	}
	return
}

func (m message) AsJson(dst any) (err error) {
	if err = m.Error(); err != nil {
		return
	}
	p, pErr := m.AsBytes()
	if pErr != nil {
		err = pErr
	}
	err = json.Unmarshal(p, dst)
	return
}
