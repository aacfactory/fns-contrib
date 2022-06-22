# Kafka
## Install
```shell
go get github.com/aacfactory/fns-contrib/message-queues/kafka
```
## Usage
```go
app.Deploy(kafka.Service())
```
## Config
```yaml
rabbitmq:
  brokers: 
    - "192.168.0.1:9093"
  producers:
    topicA:
      compression: "snappy"
      balancer: "round_robin"
      requiredAck: "one"
      batchSize: 100
      async: false
  consumers:
    topicB:
      handler: "default"
      groupId: ""
      autoCommit: false
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
published, publishErr := kafka.Publish(ctx, kafka.PublishArgument{
	Topic: "topicB"
	Key: "foo", 
	Body: json.RawMessage([]byte("{}"))
})
```