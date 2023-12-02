# MYSQL

## Usage
Install
```go
import (
    _ "github.com/aacfactory/fns-contrib/databases/sql/mysql"
)
```

## SEQUENCE
create table
```sql
CREATE TABLE `sequence` (
  `name` varchar(255) NOT NULL,
  `value` bigint NOT NULL DEFAULT '0',
  `increment` int NOT NULL DEFAULT '1',
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
```
create function
```sql
CREATE DEFINER=`root`@`%` FUNCTION `nextval`(sname varchar(255)) RETURNS bigint
    DETERMINISTIC
BEGIN
    declare next_val bigint(20);
    declare s int;
    set next_val = 0;
    select count(`name`) into s from `sequence` where `name` = sname;
    if (s = 0) then 
        insert into `sequence` values (sname, 0, 1);
    end if;
    update `sequence` set `value` = `value` + `increment` where `name` = sname;
    select `value` into `next_val` from `sequence` where `name` = sname limit 1;
RETURN next_val;
END
```