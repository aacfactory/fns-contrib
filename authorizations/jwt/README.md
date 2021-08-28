# JWT
基于 JWT 的 FNS Authorizations。
## 安装
```go
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
        "method": "",          // HS256, RS256, SOME VALUE OF ALG
        "sk": "",              // only HSXXX used
        "publicKey": "",       // pem file path
        "privateKey": "",      // pem file path
        "issuer": "", 
        "audience": [""],
        "expirations": "",     // time.Duration Formatter
        "store": {
          "kind": "",          // memory || service
          "namespace": "",     // service namespace
          "activeTokenFn": "", // fn name
          "lookUpTokenFn": "", // fn name
          "revokeTokenFn": ""  // fn name
        }
      }
    }
  }
}
```
代码注入，没有其它操作了。
```go
import _ "github.com/aacfactory/fns-contrib/authorizations/jwt"
```
## Service 类型 Store
* [redis](https://github.com/aacfactory/fns-contrib/tree/main/databases/redis)
  * namespace = redis
  * activeTokenFn = setWithTTL
  * lookUpTokenFn = contains
  * revokeTokenFn = remove

### 存储函数定义
参数，value 是 json.RawMessage，expiration 是 time.Duration。
```json
{
  "key": "",
  "value": {},
  "expiration" : 0
}
```
返回值，任何结构。
```json
{}
```
### 删除函数接口定义
参数
```json
{
  "key": ""
}
```
返回值，任何结构。
```json
{}
```
### 判断是否存在函数接口订单
参数
```json
{
  "key": ""
}
```
返回值，Bool 值。
```json
true
```
