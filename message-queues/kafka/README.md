# Kafka
## Install
```shell
go get github.com/aacfactory/fns-contrib/message-queues/kafka
```
## Usage
```go
app.Deploy(kafka.New(kafka.WithReader(name, consumer)))
```
## Config
```yaml
kafka:
  brokers: 
    - "192.168.0.1:9093"
  options:
    saslType: "plain"
    username: "user"
    password: "pass"
    clientId: "clientId"
    clientTLS:
      ca: "path of ca.pem"
      cert: "path of cert.pem"
      key: "path of key.pem"
  writer:
    topicA:
      compression: "snappy"
      balancer: "round_robin"
      requiredAck: "one"
      batchSize: 100
      async: false
  reader:
    nameA:
      groupId: ""
      autoCommit: false
```
## As proxy
```go
publishErr := kafka.Publish(ctx, "topic", kafka.NewMessage(key, body))
```