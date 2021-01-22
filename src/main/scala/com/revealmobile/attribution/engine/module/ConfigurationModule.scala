package com.revealmobile.attribution.engine.module

import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.google.inject.{AbstractModule, Provides}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import javax.inject.Singleton
import net.codingwell.scalaguice.ScalaModule
import org.slf4j.LoggerFactory

class ConfigurationModule(args: Map[String, Any]) extends AbstractModule with ScalaModule {
  private val log = LoggerFactory.getLogger(getClass)

  private val client = AWSSecretsManagerClientBuilder.standard()
    .build

  private val SnakeCaseMapper = new ObjectMapper() with ScalaObjectMapper
  SnakeCaseMapper.registerModules(DefaultScalaModule)

  private def getValue(key: String): Map[String, Any] = {
    val request = new GetSecretValueRequest().withSecretId(key)
    val json = client.getSecretValue(request).getSecretString
    SnakeCaseMapper.readValue[Map[String, Any]](json)
  }

  @Provides
  @Singleton
  def providesEnvironmentConfiguration(): Config = {
    val baseConfig = ConfigFactory.load()
    val environment = args("environment").toString
    log.info("Configured for environment: " + environment)

    val configuration = baseConfig.getConfig(environment).withFallback(baseConfig)

    environment match {
      case "local" =>
        configuration
      case _ =>
        val secretsManagerKey = configuration.getString("database.secrets_manager_key")
        val values = getValue(secretsManagerKey)
        val dbConfiguration = values.foldLeft(configuration) { (config, v) =>
          val key = v._1 match {
            case "dbname" => "properties.databaseName"
            case "username" => "properties.user"
            case "password" => "properties.password"
            case "host" => "properties.serverName"
            case "port" => "properties.port"
            case _ => v._1
          }
          config.withValue(s"database.${key}", ConfigValueFactory.fromAnyRef(v._2))
        }

        //add command line args to config
        args.foldLeft(dbConfiguration) { (config, arg) =>
          config.withValue(arg._1, ConfigValueFactory.fromAnyRef(arg._2))
        }
    }
  }

  override def configure(): Unit = {}
}
