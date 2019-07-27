package com.pubnative.domain

import play.api.libs.json.{Json, OFormat}

object Click {
  implicit val format: OFormat[Click] = Json.format[Click]
}

case class Click(impression_id: String, revenue: Double)
