package com.pubnative.recommendations

import play.api.libs.json.Json

object Recommendation {
  implicit val format = Json.format[Recommendation]
}

case class Recommendation(app_id: Option[Int], country_code: Option[String], recommended_advertiser_ids: List[Int])
