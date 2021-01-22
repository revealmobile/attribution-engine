package com.revealmobile.attribution.engine.model.project

import java.time.LocalDateTime

case class ProjectPlaceRecord(
    projectId: Long,
    chainId: Option[Long] = None,
    placeIds: Set[Long] = Set.empty,
    countryIds: Option[Set[String]] = None
)