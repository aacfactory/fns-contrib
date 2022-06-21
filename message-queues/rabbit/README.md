# RABBITMQ
## Install
```shell
go get github.com/aacfactory/fns-contrib/message-queues/rabbit
```
## Usage
```go
app.Deploy(rabbit.Service())
```
## Config
```yaml
rabbitmq:
  uri: "amqp://"
  producers:
    foo:
      exchange: "exchange"
      confirmMode: true
      key: ""
      mandatory: false
      immediate: false
      size: 8
  consumers:
    bar:
      handler: default
      queue: ""
      autoAck: false
      exclusive: false
      noLocal: false
      noWait: false
```
## Use user consumer handler
```yaml
consumers:
  bar:
    handler: user_consumer
    handlerOptions:
      userId: "userId"
```
## As proxy
```go
published, publishErr := rabbit.Publish(ctx, rabbit.PublishArgument{
	Name: "producer name", 
	Body: json.RawMessage([]byte("{}"))
})
```