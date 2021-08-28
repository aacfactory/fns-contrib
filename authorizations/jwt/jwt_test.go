package jwt_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func Test_copy(t *testing.T)  {

	p := []byte("Bearer ")
	e := []byte("123456")

	v := make([]byte, 9 + len(e))

	copy(v[:9], p)
	copy(v[9:], e)

	fmt.Println(string(v))

	fmt.Println(bytes.Index(v, p), string(v[9:]))

	exp := 30 * 24 * time.Hour
	fmt.Println(exp.String())
}
