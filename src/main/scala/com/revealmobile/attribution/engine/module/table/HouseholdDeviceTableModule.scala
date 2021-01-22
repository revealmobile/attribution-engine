package com.revealmobile.attribution.engine.module.table

import com.google.inject.{AbstractModule, Provides}
import com.revealmobile.spark.Util
import com.typesafe.config.Config
import javax.inject.Singleton
import net.codingwell.scalaguice.ScalaModule
import org.apache.spark.sql.SparkSession

class HouseholdDeviceTableModule() extends AbstractModule with ScalaModule {

  @Provides
  @Singleton
  def providesHouseholdDeviceTable(spark: SparkSession): HouseholdDeviceTable = {
    spark.sql("DROP TABLE IF EXISTS household_device_exploded")
    spark.sql(
      f"""
        CREATE EXTERNAL TABLE `household_device_exploded`(
          `ind_id` string,
          `maid` string,
          `os` string,
          `hh_id` string,
          `cookie` string,
          `other_maid` string,
          `other_os` string
        )
        STORED AS PARQUET
        LOCATION
          's3://reveal-spark/parquet/tapad_household_device_exploded'
      """
    )
    spark.sql("DROP TABLE IF EXISTS local_household_device_exploded")
    new HouseholdDeviceTable()
  }

  override def configure(): Unit = {}
}
