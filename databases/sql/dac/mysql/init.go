package mysql

// insert (conflict)
// INSERT IGNORE INTO  {table} (...) values (...)

// insert or update
// insert into {table} (...) values (...)  ON DUPLICATE KEY UPDATE ...

// insert when (not) exist
/*

INSERT INTO table (columns) SELECT values from (SELECT 1) AS __TMP__ WHERE (not) EXISTS (SELECT 1 FROM (` + "$$SOURCE_QUERY$$" + `) AS __SRC__)
*/

/*
		SELECT JSON_OBJECT
	    ('id', id,
	     'name', name,
	     'age', age, 'create_at', create_at) as ref_table
	     FROM `fns-test`.user;
*/

/*
	SELECT JSON_ARRAYAGG(
		JSON_OBJECT('id', id, 'name', name, 'age', age, 'create_at', create_at)
	)
	FROM `fns-test`.`user` AS foo FROM `fns-test`.user;
*/
