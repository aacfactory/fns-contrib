# Document
Fns document handler.

## Install
```shell
go get github.com/aacfactory/fns-contrib/transports/handlers/documents
```

## Use
```go
fns.New(fns.Handler(documents.New()))
```

## Config
```yaml
transport:
  handlers:
    documents:
      enable: true
```

## URL
Raw document
```
/documents 
```
Openapi document
```
documents?openapi=latest
```