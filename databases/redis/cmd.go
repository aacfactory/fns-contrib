package redis

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/json"
	"strconv"
	"strings"
	"time"
)

func (svc *Service) cmd(ctx fns.Context, arg OriginCommandArg) (result OriginCommandResult, err errors.CodeError) {

	params, parseErr := arg.MapToRedisDoArgs()
	if parseErr != nil {
		err = errors.ServiceError("fns Redis Cmd: map arg to redis cmd args failed").WithCause(parseErr)
		return
	}

	cmd := svc.client.Do(ctx, params)
	data, cmdErr := cmd.Result()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error()).WithCause(cmdErr)
		return
	}

	p, encodeErr := json.Marshal(data)
	if encodeErr != nil {
		err = errors.ServiceError("fns Redis Cmd: encode redis cmd result failed").WithCause(encodeErr)
		return
	}

	result = p

	return
}

func NewOriginCommandArg(name string) *OriginCommandArg {
	name = strings.TrimSpace(name)
	if name == "" {
		panic("fns Redis New Origin Cmd Arg: name is empty")
	}
	return &OriginCommandArg{
		Name: name,
		Args: OriginArgs{values: make([]string, 0, 1)},
	}
}

type OriginCommandArg struct {
	Name string     `json:"name,omitempty"`
	Args OriginArgs `json:"args,omitempty"`
}

func (arg *OriginCommandArg) AppendParams(params ...interface{}) (err error) {
	if params == nil || len(params) == 0 {
		return
	}
	for _, param := range params {
		err = arg.Args.Append(param)
		if err != nil {
			return
		}
	}
	return
}

func (arg *OriginCommandArg) MapToRedisDoArgs() (v []interface{}, err error) {
	params, paramsErr := arg.Args.convert()
	if paramsErr != nil {
		err = paramsErr
		return
	}
	v = make([]interface{}, len(arg.Args.values)+1)
	v[0] = arg.Name
	copy(v[1:], params)
	return
}

type OriginArgs struct {
	values []string
}

func (args *OriginArgs) MarshalJSON() (p []byte, err error) {
	p, err = json.Marshal(args.values)
	return
}

func (args *OriginArgs) UnmarshalJSON(p []byte) (err error) {
	err = json.Unmarshal(p, &args.values)
	return
}

func (args *OriginArgs) Append(v interface{}) (err error) {
	if v == nil {
		args.values = append(args.values, "nil:<nil>")
		return
	}
	switch v.(type) {
	case string:
		args.values = append(args.values, fmt.Sprintf("sss:%s", v.(string)))
	case []byte:
		args.values = append(args.values, fmt.Sprintf("bbb:%s", string(v.([]byte))))
	case int:
		args.values = append(args.values, fmt.Sprintf("i00:%d", v.(int)))
	case int8:
		args.values = append(args.values, fmt.Sprintf("i08:%d", v.(int8)))
	case int16:
		args.values = append(args.values, fmt.Sprintf("i16:%d", v.(int16)))
	case int32:
		args.values = append(args.values, fmt.Sprintf("i32:%d", v.(int32)))
	case int64:
		args.values = append(args.values, fmt.Sprintf("i64:%d", v.(int64)))
	case uint:
		args.values = append(args.values, fmt.Sprintf("u00:%d", v.(uint)))
	case uint8:
		args.values = append(args.values, fmt.Sprintf("u08:%d", v.(uint8)))
	case uint16:
		args.values = append(args.values, fmt.Sprintf("u16:%d", v.(uint16)))
	case uint32:
		args.values = append(args.values, fmt.Sprintf("u32:%d", v.(uint32)))
	case uint64:
		args.values = append(args.values, fmt.Sprintf("u64:%d", v.(uint64)))
	case float32:
		args.values = append(args.values, fmt.Sprintf("f32:%f", v.(float32)))
	case float64:
		args.values = append(args.values, fmt.Sprintf("f64:%f", v.(float64)))
	case bool:
		b := v.(bool)
		if b {
			args.values = append(args.values, "b00:true")
		} else {
			args.values = append(args.values, "b00:false")
		}
	case time.Time:
		t := v.(time.Time)
		args.values = append(args.values, fmt.Sprintf("ttt:%s", t.Format(time.RFC3339)))
	case time.Duration:
		d := v.(time.Duration)
		args.values = append(args.values, fmt.Sprintf("ddd:%s", d.String()))
	default:
		err = fmt.Errorf("fns Redis OriginArgs Append: appended value type is not supported")
		return
	}
	return
}

func (args *OriginArgs) convert() (v []interface{}, err error) {
	v = make([]interface{}, 0, len(args.values))
	for _, value := range args.values {
		kind := value[0:4]
		data := value[5:]
		switch kind {
		case "nil":
			v = append(v, nil)
		case "sss":
			v = append(v, data)
		case "bbb":
			v = append(v, []byte(data))
		case "i00":
			n, parseErr := strconv.Atoi(data)
			if parseErr != nil {
				err = fmt.Errorf("fns Redis OriginArgs convert: %v", parseErr)
				return
			}
			v = append(v, n)
		case "i08":
			n, parseErr := strconv.Atoi(data)
			if parseErr != nil {
				err = fmt.Errorf("fns Redis OriginArgs convert: %v", parseErr)
				return
			}
			v = append(v, int8(n))
		case "i16":
			n, parseErr := strconv.Atoi(data)
			if parseErr != nil {
				err = fmt.Errorf("fns Redis OriginArgs convert: %v", parseErr)
				return
			}
			v = append(v, int16(n))
		case "i32":
			n, parseErr := strconv.Atoi(data)
			if parseErr != nil {
				err = fmt.Errorf("fns Redis OriginArgs convert: %v", parseErr)
				return
			}
			v = append(v, int32(n))
		case "i64":
			n, parseErr := strconv.Atoi(data)
			if parseErr != nil {
				err = fmt.Errorf("fns Redis OriginArgs convert: %v", parseErr)
				return
			}
			v = append(v, int64(n))
		case "u00":
			n, parseErr := strconv.ParseUint(data, 10, 0)
			if parseErr != nil {
				err = fmt.Errorf("fns Redis OriginArgs convert: %v", parseErr)
				return
			}
			v = append(v, uint(n))
		case "u08":
			n, parseErr := strconv.ParseUint(data, 10, 0)
			if parseErr != nil {
				err = fmt.Errorf("fns Redis OriginArgs convert: %v", parseErr)
				return
			}
			v = append(v, uint8(n))
		case "u16":
			n, parseErr := strconv.ParseUint(data, 10, 0)
			if parseErr != nil {
				err = fmt.Errorf("fns Redis OriginArgs convert: %v", parseErr)
				return
			}
			v = append(v, uint16(n))
		case "u32":
			n, parseErr := strconv.ParseUint(data, 10, 0)
			if parseErr != nil {
				err = fmt.Errorf("fns Redis OriginArgs convert: %v", parseErr)
				return
			}
			v = append(v, uint32(n))
		case "u64":
			n, parseErr := strconv.ParseUint(data, 10, 0)
			if parseErr != nil {
				err = fmt.Errorf("fns Redis OriginArgs convert: %v", parseErr)
				return
			}
			v = append(v, n)
		case "f32":
			n, parseErr := strconv.ParseFloat(data, 10)
			if parseErr != nil {
				err = fmt.Errorf("fns Redis OriginArgs convert: %v", parseErr)
				return
			}
			v = append(v, float32(n))
		case "f64":
			n, parseErr := strconv.ParseFloat(data, 10)
			if parseErr != nil {
				err = fmt.Errorf("fns Redis OriginArgs convert: %v", parseErr)
				return
			}
			v = append(v, n)
		case "b00":
			if data == "true" {
				v = append(v, true)
			} else {
				v = append(v, false)
			}
		case "ttt":
			n, parseErr := time.Parse(time.RFC3339, data)
			if parseErr != nil {
				err = fmt.Errorf("fns Redis OriginArgs convert: %v", parseErr)
				return
			}
			v = append(v, n)
		case "ddd":
			n, parseErr := time.ParseDuration(data)
			if parseErr != nil {
				err = fmt.Errorf("fns Redis OriginArgs convert: %v", parseErr)
				return
			}
			v = append(v, n)
		default:
			err = fmt.Errorf("fns Redis OriginArgs convert: %s kind is not supported", kind)
			return
		}
	}
	return
}

// +-------------------------------------------------------------------------------------------------------------------+

type OriginCommandResult json.RawMessage

// As
// map to github.com/go-redis/redis/v8 result value
func (result OriginCommandResult) As(v interface{}) (err error) {
	err = json.Unmarshal(result, v)
	return
}
