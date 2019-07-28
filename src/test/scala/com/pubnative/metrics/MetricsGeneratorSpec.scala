package com.pubnative.metrics

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

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.{Source => SourceIO}

class MetricsGeneratorSpec extends WordSpec with Matchers with BeforeAndAfterAll {

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

  trait MetricGeneratorSpecData {

    implicit val actorSystem: ActorSystem = ActorSystem.create("MetricsGeneratorSpec")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val dataLoader = new DataLoader(2)
    val metricsGenerator = new MetricsGenerator(dataLoader)
    val dataWriter = new DataWriter(2, 1)
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


  "MetricsGenerator" should {
    "generate metrics by app id and country code" in new MetricGeneratorSpecData {
      generatePartitionedImpressions()
      generatePartitionedClicks()

      val metrics: List[Metric] =
        await(metricsGenerator
          .generateMetrics(Paths.get(dirPath, "impressions"), Paths.get(dirPath, "clicks"))
          .runFold(List.empty[Metric]) { (acc, metric) =>
            metric :: acc
          }
        )

      metrics should contain theSameElementsAs List(
        Metric(Some(22),Some("IT"),1,1,2.7837584244414537),
        Metric(Some(30),None,1,0,0.0),
        Metric(Some(4),Some("IT"),1,0,0.0),
        Metric(Some(9),Some(""),1,0,0.0),
        Metric(Some(32),Some("UK"),2,0,0.0)
      )
    }
  }

}
