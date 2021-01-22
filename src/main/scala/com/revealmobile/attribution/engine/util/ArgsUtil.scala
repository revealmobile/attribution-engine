package com.revealmobile.attribution.engine.util

import java.text.SimpleDateFormat
import java.util.Date
import org.rogach.scallop.{ScallopConf, ValueConverter, singleArgConverter}

object ArgsUtil {

  def pixelAttributionReportArgs(args: Seq[String]): Map[String, Any] = {
    object Conf extends ScallopConf(args) {
      implicit def dateConverter: ValueConverter[Date] = singleArgConverter[Date](new SimpleDateFormat("yyyy-MM-dd").parse(_))
      val environment = opt[String](default=Some("local"))
      val start = opt[String](required = true)
      val end = opt[String](required = true)
      val guid = opt[String](required = true)
      val project = opt[Long](required = true)
      val households = opt[Boolean](default = Some(false))
      verify
    }

    Map(
      "environment" -> Conf.environment(),
      "start" -> Conf.start(),
      "end" -> Conf.end(),
      "guid" -> Conf.guid(),
      "project_id"  -> Conf.project(),
      "include_households" -> Conf.households()
    )
  }

}
