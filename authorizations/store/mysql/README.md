# Authorizations MYSQL Store

## Usage

Make sure that sql service has been deployed.

```go
import (
	"github.com/aacfactory/fns-contrib/authorizations/store/mysql"
)

authorizations.Service(mysql.Component())
```

Config setting

```yaml
authorizations:
  store:
    schema: "schema"
    table: "table name"
```

DML

```sql
CREATE TABLE IF NOT EXISTS `YOUR TABLE` (
    `ID` VARCHAR (63),
    `USER_ID` VARCHAR (63) NOT NULL,
    `NOT_BEFORE` VARCHAR (40) NOT NULL,
    `NOT_AFTER` DATE,
    `VALUE` TEXT NOT NULL,
    PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE INDEX {YOUR TABLE}_IDX_USER_ID ON {YOUR TABLE} (USER_ID);
```