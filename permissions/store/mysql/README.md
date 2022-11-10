# Permissions MYSQL Store

## Usage

Make sure that sql service has been deployed.

```go
import (
  "github.com/aacfactory/fns-contrib/permissions/store/mysql"
)

rbac.Service(mysql.Component())
```

Config setting

```yaml
permissions:
  store:
    model: 
      schema: "schema"
      table: "table name"
    policy:
      schema: "schema"
      table: "table name"
```

## DML
Model table
```sql
CREATE TABLE IF NOT EXISTS `YOUR TABLE` (
    `NAME`      VARCHAR (255),
    `PARENT`    VARCHAR (255) NOT NULL,
    `RESOURCES` JSON,
    `VERSION`   bigint,
    PRIMARY KEY (`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```
Policy table
```sql
CREATE TABLE IF NOT EXISTS `YOUR TABLE` (
    `USER_ID`   VARCHAR (63),
    `ROLES`     VARCHAR (255) NOT NULL,
    `VERSION`   bigint,
    PRIMARY KEY (`USER_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```