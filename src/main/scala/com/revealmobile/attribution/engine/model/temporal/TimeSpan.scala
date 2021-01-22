package com.revealmobile.attribution.engine.model.temporal

import java.time.temporal.ChronoUnit

case class TimeSpan(
  `value`: Int,
  interval: ChronoUnit
)
