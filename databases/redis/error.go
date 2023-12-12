package redis

func IsRedisError(e error) (err *Error, ok bool) {
	err, ok = e.(*Error)
	return
}

type Error struct {
	message
}

func (err *Error) Error() string {
	if err.IsNil() {
		return ErrNil
	}
	return err.message.Content
}

func (err *Error) IsMoved() (addr string, ok bool) {
	if err.message.Content == ErrMoved {
		addr = err.message.Values[0].Content
		ok = true
	}
	return
}

func (err *Error) IsAsk() (addr string, ok bool) {
	if err.message.Content == ErrAsk {
		addr = err.message.Values[0].Content
		ok = true
	}
	return
}

func (err *Error) IsTryAgain() bool {

	return err.message.Content == ErrTryAgain
}

func (err *Error) IsClusterDown() bool {
	return err.message.Content == ErrClusterDown
}

func (err *Error) IsNoScript() bool {
	return err.message.Content == ErrNoScript
}
