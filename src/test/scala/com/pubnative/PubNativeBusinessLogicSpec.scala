package com.pubnative

import java.io.File
import java.util.concurrent.TimeUnit

import com.pubnative.metrics.Metric
import com.pubnative.recommendations.Recommendation
import org.scalatest.{Matchers, WordSpec}
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.io.Source

class PubNativeBusinessLogicSpec extends WordSpec with Matchers {

  trait PubNativeBusinessLogicSpecData {
    val conf = new PubNativeConf(
      Seq(
        "--impressions-dir","./src/test/resources/impressions",
        "--clicks-dir","./src/test/resources/clicks",
        "--output-dir","./target/output",
        "--top-advertiser-count","5",
        "--streams-parallelism","2",
        "--source-group-size","1"
      )
    )

    val pubNativeBusinessLogic = new PubNativeBusinessLogic(conf, 2)
    val duration = Duration(20, TimeUnit.SECONDS)

    def await[T](future: Future[T]): T = {
      Await.result(future, duration)
    }
  }

  "PubNativeBusinessLogic" should {
    "generate metrics and recommendations files" in new PubNativeBusinessLogicSpecData {
      await(pubNativeBusinessLogic.generateMetricsAndTopAdvertisers())

      val metricsJson: String =
        Source.fromFile(new File("./target/output/metrics.json")).mkString

      val recommendationsJson: String =
        Source.fromFile(new File("./target/output/recommendations.json")).mkString

      Json.parse(metricsJson).as[List[Metric]] should contain theSameElementsAs List(
        Metric(Some(32), Some("UK"), 3, 3, 5.632422825031908),
        Metric(Some(9), Some(""), 2, 1, 0.783758424441452),
        Metric(Some(4), Some("IT"), 2, 1, 2.7837584244414537),
        Metric(Some(30), None, 3, 2, 2.658520669367194),
        Metric(Some(22), Some("IT"), 2, 1, 2.783758424441452)
      )

      Json.parse(recommendationsJson).as[List[Recommendation]] should contain theSameElementsAs List(
        Recommendation(Some(32),Some("UK"), List(2, 1, 3)),
        Recommendation(Some(9),Some(""), List(11, 12)),
        Recommendation(Some(4),Some("IT"), List(7, 8)),
        Recommendation(Some(30),None, List(4, 5, 6)),
        Recommendation(Some(22),Some("IT"),List(9, 10))
      )
    }
  }

}
