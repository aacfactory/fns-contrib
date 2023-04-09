package examples_test

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/fns-contrib/transports/handlers/websockets"
	"github.com/aacfactory/fns-contrib/transports/handlers/websockets/websocket"
	"github.com/aacfactory/fns/service"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestWebsocket(t *testing.T) {
	cancel, closed, serveErr := serve(context.Background())
	if serveErr != nil {
		fmt.Println(fmt.Sprintf("%+v", serveErr))
		return
	}
	defer func(cancel context.CancelFunc, closed chan struct{}) {
		cancel()
		<-closed
		fmt.Println("websocket: closed")
	}(cancel, closed)
	echoErr := echo()
	if echoErr != nil {
		fmt.Println(fmt.Sprintf("%+v", echoErr))
		return
	}
}

func serve(ctx context.Context) (cancel context.CancelFunc, closed chan struct{}, err error) {
	beg := time.Now()
	ctx, cancel = context.WithCancel(ctx)
	app := fns.New(
		fns.Transport(fns.TransportOption().Append(websockets.Websocket())),
		fns.ConfigRetriever("./configs", "yaml", "", "fns", '-'),
	)
	deployErr := app.Deploy(EchoService())
	if deployErr != nil {
		err = deployErr
		return
	}
	runErr := app.Run(ctx)
	if runErr != nil {
		err = runErr
		return
	}
	errs := make(chan error, 1)
	go func(app fns.Application, errs chan error) {
		syncErr := app.Sync()
		if syncErr != nil {
			errs <- syncErr
		}
	}(app, errs)
	select {
	case err = <-errs:
		return
	case <-time.After(5 * time.Second):
		break
	}
	closed = make(chan struct{}, 1)
	go func(ctx context.Context, app fns.Application, closed chan struct{}) {
		<-ctx.Done()
		app.Quit()
		closed <- struct{}{}
		close(closed)
	}(ctx, app, closed)
	fmt.Println("serve: cost ", time.Now().Sub(beg.Add(5*time.Second)).String())
	return
}

func echo() (err error) {
	header := http.Header{}
	header.Set("X-Fns-Device-Id", "clientId")
	dialer := websocket.DefaultDialer
	conn, resp, dialErr := dialer.Dial("ws://127.0.0.1:18080", header)
	if dialErr != nil {
		err = errors.Warning("fns: dial failed").WithCause(dialErr)
		return
	}
	if resp.StatusCode != 200 && resp.StatusCode != 101 {
		body, bodyErr := io.ReadAll(resp.Body)
		if bodyErr != nil {
			err = errors.Warning("fns: dial failed").WithCause(bodyErr)
			return
		}
		err = errors.Warning("fns: dial failed").WithCause(errors.Warning(string(body)).WithMeta("status", resp.Status))
		return
	}
	defer conn.Close()
	for i := 0; i < 5; i++ {
		req, reqErr := websockets.NewRequest("echos", "hello", HelloParam{
			World: time.Now().Format(time.RFC3339),
		})
		if reqErr != nil {
			err = reqErr
			return
		}
		writeErr := conn.WriteJSON(req)
		if writeErr != nil {
			err = errors.Warning("fns: write failed").WithCause(writeErr)
			return
		}
		mt, p, readErr := conn.ReadMessage()
		if readErr != nil {
			err = errors.Warning("fns: read failed").WithCause(readErr)
			return
		}
		fmt.Println("client-read:", mt, string(p))
		time.Sleep(1 * time.Second)
	}

	return
}

func EchoService() service.Service {
	return &echoService{
		Abstract: service.NewAbstract("echos", false),
	}
}

type echoService struct {
	service.Abstract
}

func (svc *echoService) Build(options service.Options) (err error) {
	err = svc.Abstract.Build(options)
	return
}

func (svc *echoService) Handle(ctx context.Context, fn string, argument service.Argument) (v interface{}, err errors.CodeError) {
	svc.Log().Info().Message(fmt.Sprintf("echos: %s", fn))
	switch fn {
	case "hello":
		param := HelloParam{}
		paramErr := argument.As(&param)
		if paramErr != nil {
			err = errors.BadRequest("fns: invalid param").WithCause(paramErr)
			return
		}
		svc.Log().Info().Message(fmt.Sprintf("echo: %s", param.World))
		v = HelloResult{
			ConnId: websockets.ConnectionId(ctx),
			World:  param.World,
		}
		break
	default:
		err = errors.NotFound("fns: not found")
		break
	}
	return
}

type HelloParam struct {
	World string `json:"world"`
}

type HelloResult struct {
	ConnId string `json:"connId"`
	World  string `json:"world"`
}
