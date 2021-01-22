package com.revealmobile.attribution.engine.repository

import java.time.format.DateTimeFormatter

import com.revealmobile.attribution.engine.model.attribution.ConversionZone
import com.revealmobile.attribution.engine.model.temporal.LocalDateRange
import javax.inject.Inject
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.broadcast

class ConversionZoneVisitorRepository @Inject()(
   spark: SparkSession
  ) {

  def find(zones: Dataset[ConversionZone], range: LocalDateRange): DataFrame = {
    spark.sql("DROP TABLE IF EXISTS local_polygon_event")
    spark.sql(
      """
       CREATE EXTERNAL TABLE local_polygon_event (
        created_on timestamp,
        polygon_id bigint,
        app_id bigint,
        event_id bigint,
        geohash string,
        advertiser_id string,
        os tinyint,
        type_id tinyint,
        age smallint,
        gps_accuracy double)
     PARTITIONED BY (
      `d` string)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.mapred.TextInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
     LOCATION
       's3://reveal-spark/parquet/polygon_event'""")
    spark.sql("ALTER TABLE local_polygon_event RECOVER PARTITIONS")
    spark.table("local_polygon_event")

    val dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd")
    val from = range.from.format(dateFormat)
    val to = range.to.format(dateFormat)

    val polygonEventsDF = spark
      .table("local_polygon_event")

    polygonEventsDF
      .join(
        broadcast(zones),
        polygonEventsDF("polygon_id") === zones("polygonId")
      )
      .filter(s"d BETWEEN ${from} AND ${to}")
      .select("advertiser_id", "os")
      .distinct
      .toDF



  }

}
