# RABBITMQ
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
