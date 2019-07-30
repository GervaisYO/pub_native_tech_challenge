package com.pubnative.recommendations

import java.nio.file.{Path, Paths}
import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.pubnative.data.loader.DataLoader
import com.pubnative.data.writer.DataWriter
import com.pubnative.domain.{Click, Impression}
import org.scalatest.{Matchers, WordSpec}
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.{Source => SourceIO}

class RecommendationGeneratorSpec extends WordSpec with Matchers {

  trait RecommendationGeneratorSpecData {
    implicit val actorSystem: ActorSystem = ActorSystem.create("RecommendationGeneratorSpec")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val dataLoader = new DataLoader(2)
    val dataWriter = new DataWriter(2, 1)
    val recommendationGenerator = new RecommendationGenerator(dataLoader, dataWriter, 2)
    val dirPath = "./target"
    val partitionedImpressionsDir: Path =
      Paths.get(dirPath, s"partitioned_impressions_${Instant.now().toEpochMilli}")
    val partitionedClicksDir: Path =
      Paths.get(dirPath, s"partitioned_clicks_${Instant.now().toEpochMilli}")
    val duration = Duration(20, TimeUnit.SECONDS)

    def generatePartitionedImpressions(): Done = {
      val impressionsJson: String =
        SourceIO
          .fromFile("./src/test/resources/impressions/impressions.json")
          .mkString

      val impressionsSource = Source.fromIterator(() => Json.parse(impressionsJson).as[Iterator[Impression]])

      val result =
        dataWriter
          .writePartitions(impressionsSource, partitionedImpressionsDir)(impression => s"${impression.app_id}_${impression.country_code.getOrElse("NONE")}")
          .runWith(Sink.ignore)

      await(result)
    }

    def generatePartitionedClicks(): Done = {
      val clicksJson: String =
        SourceIO
          .fromFile("./src/test/resources/clicks/clicks.json")
          .mkString

      val clickSource = Source.fromIterator(() => Json.parse(clicksJson).as[Iterator[Click]])

      val result =
        dataWriter
          .writePartitions(clickSource, partitionedClicksDir)(click => click.impression_id)
          .runWith(Sink.ignore)

      await(result)
    }

    def await[T](future: Future[T]): T = {
      Await.result(future, duration)
    }
  }

  "RecommendationGenerator" should {
    "generate recommendation for top advertiser per app id and country code" in new RecommendationGeneratorSpecData {
      generatePartitionedImpressions()
      generatePartitionedClicks()

      val recommendations: List[Recommendation] =
        await(
          recommendationGenerator
            .generateRecommendations(partitionedImpressionsDir, partitionedClicksDir)
            .runFold(List.empty[Recommendation]) { (acc, recommendation) =>
              recommendation :: acc
            }
        )

      recommendations should contain theSameElementsAs
        List(
          Recommendation(Some(32),Some("UK"), List(2, 1)),
          Recommendation(Some(9),Some(""), List(11, 12)),
          Recommendation(Some(4),Some("IT"), List(7, 8)),
          Recommendation(Some(30),None, List(4, 5)),
          Recommendation(Some(22),Some("IT"),List(9, 10))
        )
    }
  }

}
