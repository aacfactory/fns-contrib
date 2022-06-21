# NATS.IO
## Install
```shell
go get github.com/aacfactory/fns-contrib/message-queues/nats
```
## Usage
```go
app.Deploy(nats.Service())
```
## Config
```yaml
nats:
  uri: "nats://"
  producers:
    yourSubjectName:
      size: 8
  consumers:
    bar:
      handler: default
      queue: ""
      subject: ""
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
published, publishErr := nats.Publish(ctx, nats.PublishArgument{
	Subject: "subject name", 
	Body: json.RawMessage([]byte("{}"))
})
```