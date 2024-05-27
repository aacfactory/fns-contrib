# Kafka
## Install
```shell
go get github.com/aacfactory/fns-contrib/message-queues/kafka
```
## Usage
```go
func dependencies() (v []services.Service) {
    v = []services.Service{
        // add dependencies here
		kafka.New(kafka.WithConsumeHandler(name, handleFn)),
    }
    return
}
```
## Config
```yaml
kafka:
  brokers: 
    - "192.168.0.1:9092"
  producers:
    enable: true
    num: 4
    partitioner:
      name: "round_robin"
  consumers:
    nameA:
      group: "groupId"
      topics:
        - "topicA"
```
## As proxy
```go
publishErr := kafka.Publish(ctx, kafka.NewMessage(topic, key, body))
```