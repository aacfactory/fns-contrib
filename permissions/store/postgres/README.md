# Permissions Postgres Store

## Usage

Make sure that sql service has been deployed.

```go
import (
  "github.com/aacfactory/fns-contrib/permissions/store/postgres"
)

rbac.Service(postgres.Component())
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
CREATE TABLE "{YOUR SCHEMA}"."{YOUR TABLE}"
(
    "NAME"          character varying(255) NOT NULL PRIMARY KEY,
    "PARENT"        character varying(255) NOT NULL,
    "RESOURCES"     jsonb                 NOT NULL DEFAULT '{}'::jsonb,
    "VERSION"       bigint                NOT NULL DEFAULT 0
) TABLESPACE pg_default;

ALTER TABLE IF EXISTS "YOUR SCHEMA"."YOUR TABLE" OWNER to someone;

```
Policy table
```sql
CREATE TABLE "{YOUR SCHEMA}"."{YOUR TABLE}"
(
    "USER_ID"       character varying(63) NOT NULL PRIMARY KEY,
    "ROLES"         jsonb                 NOT NULL DEFAULT '[]'::jsonb,
    "VERSION"       bigint                NOT NULL DEFAULT 0
) TABLESPACE pg_default;

ALTER TABLE IF EXISTS "YOUR SCHEMA"."YOUR TABLE" OWNER to someone;

```