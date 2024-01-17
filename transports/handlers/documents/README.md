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
      openAPI:
        version: "openapi version"
        title: ""
        description: ""
        term: ""
```

## URL
Raw document
```
/documents 
```
Openapi view (latest)
```
/documents/openapi/index.html
```
Openapi view (by version)   
It will find major and miner matched.
```
/documents/openapi/index.html?version=v1.0
```
Openapi document
```
/documents?openapi=latest
```
