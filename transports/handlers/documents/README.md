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
View
```
/documents/view/index.html
```
Raw document
```
/documents 
```
Openapi view (latest)
```
/documents/openapi/index.html
```
Openapi document
```
/documents?openapi=latest
```
