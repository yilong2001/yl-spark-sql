package com.spark.dialect.sql.util

import com.spark.dialect.sql.core.exception.ParseException
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
  * Created by yilong on 2019/5/14.
  */
object CatalogUtils {
  def convertHBaseParams(catalogTable : CatalogTable) : Map[String, String] = {
    val storageType = catalogTable.properties.get(MetaConstants.META_DATA_STORAGE_TYPE)
    if (!storageType.equals(Some(MetaConstants.META_DATA_STORAGE_TYPE_HBASE))) {
      throw new IllegalArgumentException(s"${catalogTable} : not hbase!")
    }
    val serdeParams = catalogTable.storage.properties
    val params = catalogTable.properties
    val fields = catalogTable.schema.fields
    val hbaseColsMapString = serdeParams.get("hbase.columns.mapping").get

    if (hbaseColsMapString == null) {
      throw new IllegalArgumentException(s"${catalogTable} : hbase.columns.mapping is absent!")
    }

    val allCols = hbaseColsMapString.split(",")
    if (allCols.length > catalogTable.schema.fields.length) {
      throw new IllegalArgumentException(s"${catalogTable} : hbase.columns.mapping cols is more than table metadata!")
    }

    //TODO:
    null
  }
}
