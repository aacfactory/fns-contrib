# SQL

在proxy中增加 tx 的选择，当ctx 的meta 中 database_xid 时，其value为proxy的id，然后选择该id的proxy
进行代理。