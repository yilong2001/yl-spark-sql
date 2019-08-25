-- (1) snapshot
-- hive snapshot
MAKE SNAPSHOT snapshot_pp83
DIRECTORY '/tmp/yl-spark-sql/datasource'
FROM p_p83
PARTITION (age=21, sex='F')
PARTITION (age=25, sex='M')

-- hbase snapshot
create external table hbase_myrecord_t1(rowkey STRING,uid STRING,nick STRING,grade INT)
STORED BY 'org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler'
WITH SERDEPROPERTIES()
TBLPROPERTIES("spark.dialect.hbase.table.name"="myrecord_t1",
"spark.dialect.hbase.zookeeper.quorum"="localhost",
"spark.dialect.hbase.zookeeper.property.clientPort"="2181",

"spark.dialect.hbase.columns.mapping"=":key,f:uid,f:nick,f:grade"
"spark.dialect.hbase.table.rowkey.fields.first"="uid"
-- "spark.dialect.hbase.table.rowkey.prefix"="00",
-- "spark.dialect.hbase.table.rowkey.prefix.type"="byte", -- byte or string
"spark.dialect.hbase.table.rowkey.func"="concat(reverse(uid),'_',substring(nick,0,10))"
)

make snapshot myrecord_t1_snapshot from hbase_myrecord_t1

-- insert into hbase from hive through by hfile
insert into hbase_myrecord_t1 select 'uid_a100001_nick_a100001'
as rowkey, 'uid_a100001' as uid, 91 as grade from demo_parquet1

-- (2) update
-- TODO: (1) update tablename (partition A, partition B...) ...
-- TODO: (2) 对于 hive table， 在 update 完成之后，要确保角色、权限依然保持原状态
-- TODO: (3) 如果备份了原始数据，要定期清理???
-- update sql
-- local example
CREATE TEMPORARY VIEW p01 (name STRING, age INT,job STRING) USING CSV OPTIONS
 (sep ';' , header 'true' , path '/tmp/yl-spark-sql/datas/source/p01.csv')

CREATE TEMPORARY VIEW p02 (name STRING, age INT,job STRING) USING CSV OPTIONS
(sep ';' , header 'true' , path '/tmp/yl-spark-sql/datas/source/p02.csv')

CREATE TEMPORARY VIEW p03 (name STRING, age INT,job STRING) USING CSV OPTIONS
(sep ';' , header 'true' , path '/tmp/yl-spark-sql/datas/source/p03.csv')

UPDATE p01 SET age = tm.age + 2, job = concat(tm.job,'_succ') FROM
(select p03.name as name1, p03.age as age, p03.job as job, p02.name as name2 from p02, p03 where p02.name = p03.name) tm
WHERE p01.name = tm.name1 and p01.name = concat(tm.name1,'')

UPDATE p01 SET age = p03.age + 2, job = concat(p03.job,'_good') FROM p03
WHERE p01.name = p03.name

-- hive example
CREATE EXTERNAL TABLE p01 (name STRING, age INT,job STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/user/e_p01/'
--TBLPROPERTIES ('header' = 'true', 'inferSchema' = 'false', 'skip.header.line.count' = '1');
TBLPROPERTIES ('header' = 'false', 'inferSchema' = 'true');

CREATE EXTERNAL TABLE p02 (name STRING, age INT,job STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/e_p02/'
TBLPROPERTIES ('header' = 'true', 'inferSchema' = 'false', 'skip.header.line.count' = '1');

CREATE EXTERNAL TABLE p03 (name STRING, age INT,job STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/e_p03/'
TBLPROPERTIES ('header' = 'true', 'inferSchema' = 'false', 'skip.header.line.count' = '1');



-- (3) delete
-- TODO: delete (like update); delete ... (partition A, partition B...) ...

-- train
--
