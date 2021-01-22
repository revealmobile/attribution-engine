package com.revealmobile.attribution.engine.module

import com.google.inject.{AbstractModule, Provides}
import com.revealmobile.spark.Util
import com.typesafe.config.Config
import javax.inject.Singleton
import net.codingwell.scalaguice.ScalaModule

class SparkJDBCModule(job: String) extends AbstractModule with ScalaModule {

  @Provides
  @Singleton
  def providesSparkJDBC(config: Config): (SparkJDBC) = {

    val dbHost = config.getString("database.properties.serverName")
    if (!dbHost.isEmpty) {
      val dbPort = config.getInt("database.properties.port")
      val dbName = config.getString("database.properties.databaseName")
      val jdbcProps = new java.util.Properties
      jdbcProps.setProperty("user", config.getString("database.properties.user"))
      jdbcProps.setProperty("password", config.getString("database.properties.password"))
      jdbcProps.setProperty("driver", "org.postgresql.Driver")

      new SparkJDBC(s"jdbc:postgresql://${dbHost}:${dbPort}/${dbName}", jdbcProps)
    } else {
      val (url, props) = Util.loadJDBCConfig(job)
      new SparkJDBC(url, props)
    }

  }

  override def configure(): Unit = {}
}
