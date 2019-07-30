package com.pubnative.data.writer

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.pubnative.domain.{Click, Impression}
import org.scalatest.{Matchers, WordSpec}
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.{Source => SourceIO}

class DataWriterSpec extends WordSpec with Matchers {

  trait DataWriterSpecData {
    implicit val actorSystem: ActorSystem = ActorSystem.create("DataWriterSpec")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val dataWriter = new DataWriter(2, 1)
    val dirPath = "./target"

    val duration = Duration(20, TimeUnit.SECONDS)
    val partitionedImpressionsDir: Path = Paths.get(dirPath, s"partitioned_impressions_${Instant.now().toEpochMilli}")
    val partitionedClicksDir: Path = Paths.get(dirPath, s"partitioned_clicks_${Instant.now().toEpochMilli}")

    def await[T](future: Future[T]): T = {
      Await.result(future, duration)
    }
  }

  "DataWriter" should {
    "write partitioned impressions by app_id and country_code" in new DataWriterSpecData {
      val impressionsJson: String =
        SourceIO
          .fromFile("./src/test/resources/impressions/impressions.json")
          .mkString

      val impressionsSource: Source[Impression, NotUsed] = Source.fromIterator(() => Json.parse(impressionsJson).as[Iterator[Impression]])

      val result: Future[Done] =
        dataWriter
          .writePartitions(impressionsSource, partitionedImpressionsDir)(impression => s"${impression.app_id}_${impression.country_code.getOrElse("NONE")}")
          .runWith(Sink.ignore)

      await(result)

      Files.exists(Paths.get(s"${partitionedImpressionsDir.toFile.getAbsolutePath}/32_UK")) shouldBe true
      Files.exists(Paths.get(s"${partitionedImpressionsDir.toFile.getAbsolutePath}/30_NONE")) shouldBe true
      Files.exists(Paths.get(s"${partitionedImpressionsDir.toFile.getAbsolutePath}/4_IT")) shouldBe true
      Files.exists(Paths.get(s"${partitionedImpressionsDir.toFile.getAbsolutePath}/22_IT")) shouldBe true
      Files.exists(Paths.get(s"${partitionedImpressionsDir.toFile.getAbsolutePath}/9_")) shouldBe true

      new File(s"${partitionedImpressionsDir.toFile.getAbsolutePath}/32_UK").listFiles().length shouldBe 3
      new File(s"${partitionedImpressionsDir.toFile.getAbsolutePath}/30_NONE").listFiles().length shouldBe 3
      new File(s"${partitionedImpressionsDir.toFile.getAbsolutePath}/4_IT").listFiles().length shouldBe 2
      new File(s"${partitionedImpressionsDir.toFile.getAbsolutePath}/22_IT").listFiles().length shouldBe 2
      new File(s"${partitionedImpressionsDir.toFile.getAbsolutePath}/9_").listFiles().length shouldBe 2
    }

    "write partitioned clicks by impression_id" in new DataWriterSpecData {
      val clicksJson: String =
        SourceIO
          .fromFile("./src/test/resources/clicks/clicks.json")
          .mkString

      val clicksSource: Source[Click, NotUsed] = Source.fromIterator(() => Json.parse(clicksJson).as[Iterator[Click]])

      val result: Future[Done] =
        dataWriter
          .writePartitions(clicksSource, partitionedClicksDir)(click => click.impression_id)
          .runWith(Sink.ignore)

      await(result)

      Files.exists(Paths.get(s"${partitionedClicksDir.toFile.getAbsolutePath}/a39747e8-9c58-41db-8f9f-27963bc248b5")) shouldBe true
      Files.exists(Paths.get(s"${partitionedClicksDir.toFile.getAbsolutePath}/a39747e8-9c58-41db-8f9f-27963bc248b6")) shouldBe true
      Files.exists(Paths.get(s"${partitionedClicksDir.toFile.getAbsolutePath}/a39747e8-9c58-41db-8f9f-27963bc248b7")) shouldBe true
      Files.exists(Paths.get(s"${partitionedClicksDir.toFile.getAbsolutePath}/5deacf2d-833a-4549-a398-20a0abeec0bc")) shouldBe true
      Files.exists(Paths.get(s"${partitionedClicksDir.toFile.getAbsolutePath}/2ae9fd3f-4c70-4d9f-9fe0-98cb2f0b7521")) shouldBe true
      Files.exists(Paths.get(s"${partitionedClicksDir.toFile.getAbsolutePath}/fbb52038-4db1-46d3-a4de-108fd12cbfc7")) shouldBe true
      Files.exists(Paths.get(s"${partitionedClicksDir.toFile.getAbsolutePath}/b15449b6-14c9-406b-bce9-749805dd6a3e")) shouldBe true
      Files.exists(Paths.get(s"${partitionedClicksDir.toFile.getAbsolutePath}/5deacf2d-833a-4549-a398-20a0abeec0bt")) shouldBe true

      new File(s"${partitionedClicksDir.toFile.getAbsolutePath}/a39747e8-9c58-41db-8f9f-27963bc248b5").listFiles().length shouldBe 1
      new File(s"${partitionedClicksDir.toFile.getAbsolutePath}/a39747e8-9c58-41db-8f9f-27963bc248b6").listFiles().length shouldBe 1
      new File(s"${partitionedClicksDir.toFile.getAbsolutePath}/a39747e8-9c58-41db-8f9f-27963bc248b7").listFiles().length shouldBe 1
      new File(s"${partitionedClicksDir.toFile.getAbsolutePath}/5deacf2d-833a-4549-a398-20a0abeec0bc").listFiles().length shouldBe 1
      new File(s"${partitionedClicksDir.toFile.getAbsolutePath}/2ae9fd3f-4c70-4d9f-9fe0-98cb2f0b7521").listFiles().length shouldBe 1
      new File(s"${partitionedClicksDir.toFile.getAbsolutePath}/fbb52038-4db1-46d3-a4de-108fd12cbfc7").listFiles().length shouldBe 1
      new File(s"${partitionedClicksDir.toFile.getAbsolutePath}/b15449b6-14c9-406b-bce9-749805dd6a3e").listFiles().length shouldBe 1
      new File(s"${partitionedClicksDir.toFile.getAbsolutePath}/5deacf2d-833a-4549-a398-20a0abeec0bt").listFiles().length shouldBe 1
    }
  }

}
