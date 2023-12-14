package sql_test

import (
	"fmt"
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
		sql.NewNullString("ns"),
		sql.NewNullBool(true),
		sql.NewNullInt64(1),
		sql.NewNullFloat64(13),
		sql.NewNullDatetime(time.Now()),
		sql.NewNullJson[time.Time](time.Now()),
	}

	args := sql.Arguments(srs)

	p, encodeErr := json.Marshal(args)
	if encodeErr != nil {
		t.Errorf("%+v", encodeErr)
		return
	}
	fmt.Println(string(p))

	var arguments sql.Arguments
	decodeErr := json.Unmarshal(p, &arguments)
	if decodeErr != nil {
		t.Errorf("%+v", decodeErr)
		return
	}
	for _, argument := range arguments {
		switch x := argument.(type) {
		case []byte:
			fmt.Println("bytes", string(x))
			break
		case byte:
			fmt.Println("byte", string(x))
			break
		default:
			fmt.Println(fmt.Sprintf("%+v", argument), reflect.ValueOf(x).Type())
			break
		}
	}
}
