package com.revealmobile.attribution.engine.module

import com.google.inject.{AbstractModule, Provides}
import com.typesafe.config.Config
import javax.inject.Singleton
import net.codingwell.scalaguice.ScalaModule
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._


class SparkModule extends AbstractModule with ScalaModule {

  @Provides
  @Singleton
  def providesSparkSession(configuration: Config): SparkSession = {

    val spark = SparkSession
      .builder
      .master(configuration.getString("spark.master"))
      .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
      .enableHiveSupport
      .getOrCreate

    spark
  }


  override def configure(): Unit = {}
}

