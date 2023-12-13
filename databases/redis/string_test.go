package redis_test

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/redis"
	"github.com/aacfactory/fns/tests"
	"testing"
	"time"
)

func TestSet(t *testing.T) {
	setupErr := setup()
	if setupErr != nil {
		t.Error(fmt.Sprintf("%+v", setupErr))
		return
	}
	defer tests.Teardown()
	ctx := tests.TODO()
	r, setErr := redis.Do(ctx, redis.Set("some", time.Now().Format(time.RFC3339)).Ex(10*time.Second))
	if setErr != nil {
		t.Error(fmt.Sprintf("%+v", setErr))
		return
	}
	if r.Error() != nil {
		t.Error(fmt.Sprintf("%+v", r.Error()))
		return
	}
}

func TestGet(t *testing.T) {
	setupErr := setup()
	if setupErr != nil {
		t.Error(fmt.Sprintf("%+v", setupErr))
		return
	}
	defer tests.Teardown()
	ctx := tests.TODO()
	r, getErr := redis.Do(ctx, redis.Get("some"))
	if getErr != nil {
		t.Error(fmt.Sprintf("%+v", getErr))
		return
	}
	if r.IsNil() {
		t.Log("nil")
		return
	}
	s, sErr := r.AsString()
	if sErr != nil {
		t.Error(fmt.Sprintf("%+v", sErr))
		return
	}
	t.Log(s)
}

func TestIncr(t *testing.T) {
	setupErr := setup()
	if setupErr != nil {
		t.Error(fmt.Sprintf("%+v", setupErr))
		return
	}
	defer tests.Teardown()
	ctx := tests.TODO()
	r, getErr := redis.Do(ctx, redis.Incr("some_incr"))
	if getErr != nil {
		t.Error(fmt.Sprintf("%+v", getErr))
		return
	}
	if r.IsNil() {
		t.Log("nil")
		return
	}
	s, sErr := r.AsInt()
	if sErr != nil {
		t.Error(fmt.Sprintf("%+v", sErr))
		return
	}
	t.Log(s)
}

func TestDel(t *testing.T) {
	setupErr := setup()
	if setupErr != nil {
		t.Error(fmt.Sprintf("%+v", setupErr))
		return
	}
	defer tests.Teardown()
	ctx := tests.TODO()
	r, getErr := redis.Do(ctx, redis.Del("some_incr"))
	if getErr != nil {
		t.Error(fmt.Sprintf("%+v", getErr))
		return
	}
	if r.Error() != nil {
		t.Error(fmt.Sprintf("%+v", r.Error()))
		return
	}
}

func TestMGet(t *testing.T) {
	setupErr := setup()
	if setupErr != nil {
		t.Error(fmt.Sprintf("%+v", setupErr))
		return
	}
	defer tests.Teardown()
	ctx := tests.TODO()
	for i := 0; i < 3; i++ {
		_, setErr := redis.Do(ctx, redis.Set(fmt.Sprintf("some_%d", i), time.Now().Format(time.RFC3339)).Ex(10*time.Second))
		if setErr != nil {
			t.Error(fmt.Sprintf("%+v", setErr))
			return
		}
	}
	r, gerErr := redis.Do(ctx, redis.MGet("some_0", "some_1", "some_2"))
	if gerErr != nil {
		t.Error(gerErr)
		return
	}
	if r.IsNil() {
		t.Log("nil")
		return
	}
	ss, ssErr := r.AsStrSlice()
	if ssErr != nil {
		t.Error(ssErr)
		return
	}
	t.Log(ss)
}
