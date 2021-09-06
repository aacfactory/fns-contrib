# JWT
基于 JWT 的 FNS Authorizations。
## 安装
```shell
go get github.com/aacfactory/fns-contrib/authorizations/jwt@main
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
## Store
令牌存储器，用于存储令牌，一般用于单点登录（一个令牌只能在被申请终端设备上使用），直接吊销等功能。
### Service 类型 Store
* [redis](https://github.com/aacfactory/fns-contrib/tree/main/databases/redis)
  * namespace = redis
  * activeTokenFn = set
  * lookUpTokenFn = contains
  * revokeTokenFn = remove

### 保存函数定义
参数：key 是 JTI，value 是 json.RawMessage 类型的 token，expiration 是 time.Duration。
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
参数：key 是 JTI
```json
{
  "key": ""
}
```
返回值，任何结构。
```json
{}
```
### 判断是否存在函数接口定义
参数：key 是 JTI
```json
{
  "key": ""
}
```
返回值，Bool 值。
```json
true
```
