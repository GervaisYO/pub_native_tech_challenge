package com.pubnative.domain

import play.api.libs.json.{Json, OFormat}


object Impression {
  implicit val format: OFormat[Impression] = Json.format[Impression]
}

case class Impression(id: String, app_id: Int, country_code: Option[String], advertiser_id: Int)
