package sql_test

import (
	"fmt"
	"github.com/aacfactory/avro"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns/commons/times"
	"github.com/aacfactory/json"
	"reflect"
	"testing"
	"time"
)

func TestNewArgument(t *testing.T) {
	srs := []any{
		"string", true, int64(1), float64(12), time.Now(), times.DateNow(), times.TimeNow(), byte(','), []byte("bytes"), json.RawMessage("json"),
		sql.NewNullString(""),
		sql.NewNullBool(true),
		sql.NewNullInt64(1),
		sql.NewNullFloat64(13),
		sql.NewNullDatetime(time.Time{}),
		sql.NewNullJson[time.Time](time.Now()),
	}

	args := sql.Arguments(srs)

	p, encodeErr := avro.Marshal(args)
	if encodeErr != nil {
		t.Errorf("%+v", encodeErr)
		return
	}
	arguments := make(sql.Arguments, 0, 1)
	decodeErr := avro.Unmarshal(p, &arguments)
	if decodeErr != nil {
		t.Errorf("%+v", decodeErr)
		return
	}
	for _, argument := range arguments {
		switch x := argument.(type) {
		case nil:
			fmt.Println("nil")
		case []byte:
			fmt.Println("bytes", string(x))
			break
		case byte:
			fmt.Println("byte", string(x))
			break
		default:
			fmt.Println(reflect.ValueOf(x).Type(), fmt.Sprintf("%+v", argument))
			break
		}
	}
}
