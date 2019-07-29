package com.pubnative.recommendations

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.pubnative.data.loader.DataLoader
import com.pubnative.data.writer.DataWriter
import com.pubnative.domain.{Click, Impression}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import play.api.libs.json.Json

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.io.{Source => SourceIO}
import scala.concurrent.ExecutionContext.Implicits.global

class RecommendationGeneratorSpec extends WordSpec with Matchers with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    super.afterAll()
    for {
      rootDir <- List(Paths.get("./target", "impressions"), Paths.get("./target", "clicks"))
      rootDirWithSubDir <- rootDir.toFile.listFiles().toSeq :+ rootDir.toFile
      file <- rootDirWithSubDir.listFiles().toSeq :+ rootDirWithSubDir
    } yield {
      Files.delete(Paths.get(file.getAbsolutePath))
    }
  }

  trait RecommendationGeneratorSpecData {
    implicit val actorSystem: ActorSystem = ActorSystem.create("MetricsGeneratorSpec")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val dataLoader = new DataLoader(2)
    val dataWriter = new DataWriter(2, 1)
    val recommendationGenerator = new RecommendationGenerator(dataLoader, dataWriter, 2)
    val dirPath = "./target"
    val duration = Duration(20, TimeUnit.SECONDS)

    def generatePartitionedImpressions(): Done = {
      val impressionsJson: String =
        SourceIO
          .fromFile("./src/test/resources/impressions.json")
          .mkString

      val impressionsSource = Source.fromIterator(() => Json.parse(impressionsJson).as[Iterator[Impression]])

      val result =
        dataWriter
          .writePartitions(impressionsSource, Paths.get(dirPath, "impressions"))(impression => s"${impression.app_id}_${impression.country_code.getOrElse("NONE")}")
          .runWith(Sink.ignore)

      await(result)
    }

    def generatePartitionedClicks(): Done = {
      val clicksJson: String =
        SourceIO
          .fromFile("./src/test/resources/clicks.json")
          .mkString

      val clickSource = Source.fromIterator(() => Json.parse(clicksJson).as[Iterator[Click]])

      val result =
        dataWriter
          .writePartitions(clickSource, Paths.get(dirPath, "clicks"))(click => click.impression_id)
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

      val recommendations =
        await(
          recommendationGenerator
            .generateRecommendations(Paths.get(dirPath, "impressions"), Paths.get(dirPath, "clicks"))
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
