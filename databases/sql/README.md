# SQL

在proxy中增加 tx 的选择，当ctx 的meta 中 database_xid 时，其value为proxy的id，然后选择该id的proxy
进行代理。

当有database_xid时，在 写操作的时，如果 proxy 为空（可能超时等等）都failed。

/*
cache
map【key】tx
rw
timeoutCloseCh

tx{
tx
timeoutCloseCh <-
timer
}

分布式 tx，在begin后，返回 publicHost:publicPort
*/