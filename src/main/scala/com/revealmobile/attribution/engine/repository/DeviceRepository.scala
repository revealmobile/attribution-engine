package com.revealmobile.attribution.engine.repository

import javax.inject.Inject
import org.apache.spark.sql.{DataFrame, SparkSession}

class DeviceRepository @Inject()(spark: SparkSession) {

  def list: DataFrame = {
    spark.sql("DROP TABLE IF EXISTS local_daily_device")
    spark.sql(
      """
    CREATE EXTERNAL TABLE local_daily_device (
      advertiser_id string,
      os tinyint,
      app_id bigint,
      event_count bigint,
      countries array<string>)
       PARTITIONED BY (
      year smallint, month tinyint, day tinyint
      )
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.mapred.TextInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
     LOCATION
       's3://reveal-spark/parquet/daily_device_app'
      """)
    spark.sql("ALTER TABLE local_daily_device RECOVER PARTITIONS")

    spark.sql(
      "SELECT * FROM local_daily_device WHERE year = 2020" //todo make this configurable
    )

  }

}
