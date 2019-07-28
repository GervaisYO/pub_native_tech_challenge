package com.pubnative.metrics

import play.api.libs.json.{Json, OFormat}

object Metric {
  implicit val format: OFormat[Metric] = Json.format[Metric]

  val empty = Metric(None, None, 0, 0, 0.0)

  def combine(metric1: Metric, metric2: Metric): Metric = {
    Metric(
      metric2.app_id,
      metric2.country_code,
      metric1.impressions + metric2.impressions,
      metric1.clicks + metric2.clicks,
      metric1.revenue + metric2.revenue
    )
  }
}

case class Metric(app_id: Option[Int],
                  country_code: Option[String],
                  impressions: Int,
                  clicks: Int,
                  revenue: Double)
