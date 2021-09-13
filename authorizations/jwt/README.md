# JWT
基于 JWT 的 FNS Authorizations。
## 安装
```shell
go get github.com/aacfactory/fns-contrib/authorizations/jwt
```
## 使用
配置文件，其中kind必须是jwt，且必须小写。
```json
{
  "services": {
    "authorization": {
      "enable": true,
      "kind": "jwt",
      "config": {
        "method": "",          // HS256, RS512, SOME VALUE OF ALG
        "sk": "",              // only HSXXX used
        "publicKey": "",       // pem file path
        "privateKey": "",      // pem file path
        "issuer": "", 
        "audience": [""],
        "expirations": ""     // time.Duration Formatter
      }
    }
  }
}
```
代码注入，没有其它操作了。
```go
import _ "github.com/aacfactory/fns-contrib/authorizations/jwt"
```
