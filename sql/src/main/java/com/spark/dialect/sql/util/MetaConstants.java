package com.spark.dialect.sql.util;

import org.apache.commons.lang.StringUtils;

/**
 * Created by yilong on 2019/5/9.
 */
public class MetaConstants {
    static public final String META_DATA_PUBLIC_PREFIX = "spark.dialect";
    //storage : default is hive, others are hbase, es, solr, redis, oracle, mysql, ...
    static public final String META_DATA_STORAGE_TYPE = META_DATA_PUBLIC_PREFIX + ".storage.type";

    static public final String META_DATA_STORAGE_TYPE_HBASE = "hbase";
    static public final String META_DATA_STORAGE_TYPE_HIVE = "hive";
    static public final String META_DATA_STORAGE_TYPE_ES = "es";
    static public final String META_DATA_STORAGE_TYPE_SOLR = "solr";
    static public final String META_DATA_STORAGE_TYPE_ORACLE = "oracle";
    static public final String META_DATA_STORAGE_TYPE_REDIS = "redis";
    static public final String META_DATA_STORAGE_TYPE_MYSQL = "mysql";

    static public final String META_DATA_HBASE_TABLE_NAME = META_DATA_PUBLIC_PREFIX + ".hbase.table.name";
    static public final String META_DATA_HBASE_ZOOKEEPER = META_DATA_PUBLIC_PREFIX + ".hbase.zookeeper.quorum";
    static public final String META_DATA_HBASE_ZOOKEEPER_PORT = META_DATA_PUBLIC_PREFIX + ".hbase.zookeeper.property.clientPort";

    static public final String META_DATA_HBASE_ROWKEY_FIELDS_FIRST = META_DATA_PUBLIC_PREFIX + ".hbase.table.rowkey.fields.first";

    static public final String META_DATA_HBASE_COLUMNS_MAPPING = META_DATA_PUBLIC_PREFIX + ".hbase.columns.mapping";
    static public final String META_DATA_HBASE_TABLE_ROWKEY_FUNC = META_DATA_PUBLIC_PREFIX + ".hbase.table.rowkey.func";

    static public final String MEAT_DATA_TRAIN_MODEL_ALGORITHM_SERVERS = META_DATA_PUBLIC_PREFIX + ".train.model.algorithm.servers";

    static public String backupDirBeforeUpdate(String userHome, String appid, String date) {
        String[] strArray={userHome, "_backup_", date, appid};
        return StringUtils.join(strArray, "/");
    }


}
