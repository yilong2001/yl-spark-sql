# yl-spark-sql
一种基于 Spark 的 SQL 方言，扩展和增强了 SQL 语法，以支持批处理、机器学习、模型服务、以及流处理的一个 SQL 框架。

## lambda 架构的问题
lambda 三层架构：batch layer、speed layer、serving layer，三层独立。

不同层组件不一、开发语言不一：如 spark、flink、storm、hive、hbase、es、pg、gp、mongodb，以及各种机器学习框架、各种开发语言等。对开发、维护来说，都带来很多额外的成本。

## 机器学习过程中的问题
```
数据采集 --> 数据清洗 --> 训练模型 --> 部署模型 
   <|>                                 |
    |__________________________________|   
    
这个过程中的问题：
1) 寻找特征的过程，数据需要反复清洗，清洗和建模是割裂的
2) 数据科学家训练出来的模型，要开发人员工程化落地
3) 模型服务也需要独立开发
4) 多种机器学习框架，如 spark mllib、sklearn、tensorflow、mxnet 等，要融合利用起来，学习成本巨大
5) 数据数据科学家侧重灵活、工程团队侧重效率，在语言、工具的选择上也各有侧重

```

## 目标
语言层面：增强SQL，以支持 流处理、批处理、机器学习、模型服务等多种场景

工作流层面：有相应的调度系统，支持 ETL、数据清洗、科学分析、模型应用等多种阶段

## SQL 方言(增强和扩展)
```
基于 SQL 开发方言：
1) 简单易学，算法专家、数据科学家、研发工程师、产品经理都能使用
2) 易于扩展

增强和扩展：
1) 扩展 snapshot、restore、update、delete，增强 select 、insert 等批量计算方面的语义;
2) 扩展 train、predict、register、deploy 等机器学习、模型服务方面的语义;
3) 同时，需要重写 HBase、ES、pg、mongodb、solr 等存储的 scan 和 write 接口，结合 SQL 方言，提高查询效率；
4) 与 mybatis 等 ORM 框架集成;

```

## 批处理
1、以备份为例；
```
HBase创建快照：
snapshot ‘tableName’, ‘snapshotName’

Solr备份：
http://xxx/solr/coreName/replication?command=backup

Elasticsearch创建快照：
curl -XPUT http://xxx/_snapshot/my_backup/snapshotName { "indices": "index_1, index_2" }

```

使用 SQL 方言可以简化为：
```
make snapshot snapshotName from sourceTable
```
当然，需要提前创建外部表：
```
create external table extern_hbase_t1(rowkey STRING,uid STRING,nick STRING,grade INT)
STORED BY 'org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler'
WITH SERDEPROPERTIES( "hbase.columns.mapping"=":key,f:uic,f:nick,f:grade")
TBLPROPERTIES(
"spark.dialect.storage.type"="hbase", -- 可以是 es / solr / mongodb 等等
"spark.dialect.hbase.table.name"="hbase_t1",
"spark.dialect.hbase.zookeeper.quorum"="localhost",
"spark.dialect.hbase.zookeeper.property.clientPort"="2181")
```

2、以 hive to hbase 为例：
```
使用 bulkload 机制，数据从 hive 导入 hbase 的步骤如下：
1) 创建一个outputformat为HiveHFileOutputFormat的hive表(生成hfile)
2) 原始数据表的数据，通过hbase_hfile_table表保存为hfile
3) 确保对应hdfs路径，生成了hfile
4) 通过bulkload将数据导入到hbase表中

而且：在这个过程中的问题，每个应用都会遇到问题，都要解决这些问题

```

使用 SQL 方言可简化为：
```
1) set hivetohbase.type=bulkload; -- 如果设置为 auto，可以根据数据量自适应选择导入方式，by record or bulkload
2) 然后：Insert into … 

```

3、增强 update / delete 
(1) 增强 update / delete 语法

(2) 支持 hive、ES、HBase、kudu 等存储

(3) 支持不同存储关联查询后的更新/删除

SQL 示例如下：
```
其中：p01 / p02 / p03 都是 hive 表

UPDATE p01 SET age = tm.age + 2, job = concat(tm.job,'_succ') FROM 
 (select p03.name as name1, p03.age as age, p03.job as job, p02.name as name2 
  from p02 
  inner join p03
  on p02.name = p03.name) tm 
WHERE p01.name = tm.name1 and p01.name = concat(tm.name1,'') 
```

## 机器学习

### 步骤：基于 SQL 方言
```
1) 创建外部表，存储类型可以是：HBase/ES/MySQL/文件/hive等
2) 使用 SQL 进行数据清洗，完成关联、聚合、处理
3) 使用 TRAIN SQL 选择算法，训练模型
5) 使用REGISTER SQL 注册模型
6) 使用 SQL / UDF 对数据进行预测，验证或应用模型

```

### train sql 示例：
```
TRAIN MODEL LogisticRegression FROM (
select step,
 getTypeCode(type) as tcd,
 amount,
 getOldBalanceOrigValue(oldbalanceOrg,newbalanceOrig,amount) as oldbalanceOrg,
	getNewBalanceOrigValue(oldbalanceOrg,newbalanceOrig,amount) as newbalanceOrig,
	getOldBalanceDestValue(oldbalanceDest,newbalanceDest,amount) as oldbalanceDest,
	getNewBalanceDestValue(oldbalanceDest,newbalanceDest,amount) as newbalanceDest,
 (newbalanceOrig+amount-oldbalanceOrg) as errorBalanceOrig,
 (oldbalanceDest+amount-newbalanceDest) as errorBalanceDest,
 isFraud as label
 from p_part_log
 where type = 'TRANSFER' or type = 'CASH_OUT'
) p_part
INTO
'/tmp/models/lrmodel.m'
```

### register sql 示例：
```
 register model LogisticRegression
 FROM '/tmp/models/lrmodel.m'
 AS lr_predict
```

### predict sql 示例：
```
select label, lr_predict((step,tcd,amount,oldbalanceOrg,newbalanceOrig,oldbalanceDest,newbalanceDest,errorBalanceOrig,errorBalanceDest)) as predict 
  from tmp_p_part_log
```

### UDF 示例：
```
计算向量中的最大值
Select vec_max(vec_dense(array(0.1, 0.5, 0.8)))

计算向量中的最大值所在的 index
Select vec_argmax(vec_dense(array(0.1, 0.5, 0.8)))
```

### 计算框架

1、Spark MLlib

2、scikit-learn

3、TensorFlow

4、支持自定义算法

5、可以支持更多：Keras、Caffe2、CNTK 等

## 模型服务
### 模型服务框架
```
Clipper： RISE Lab开发， 在应用和机器学习模型之间提供预测服务
Seldon：在Kubernetes上部署机器学习模型
Mlflow：Databricks开发，用于管理机器学习端到端生命周期
Mleap：Spark 与 SKLearn 之间的格式转换库
PredictionIO：开源机器学习服务应用
其他...
```
### SQL 方言 示例：
```
deploy model LogisticRegression FROM ‘/tmp/models/lrmodel.m’ to gatewayName 
options (…)
```

## 与 mybatis 集成
TBD


wx: yilong2001


