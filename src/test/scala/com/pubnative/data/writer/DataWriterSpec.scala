package com.pubnative.data.writer

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.pubnative.domain.{Click, Impression}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.{Source => SourceIO}

class DataWriterSpec extends WordSpec with Matchers with BeforeAndAfterAll {

  val partitionedDirs =
    List(
      "./target/32_UK",
      "./target/30_NONE",
      "./target/4_IT",
      "./target/22_IT",
      "./target/9_",
      "./target/a39747e8-9c58-41db-8f9f-27963bc248b5",
      "./target/a39747e8-9c58-41db-8f9f-27963bc248b6",
      "./target/a39747e8-9c58-41db-8f9f-27963bc248b7",
      "./target/5deacf2d-833a-4549-a398-20a0abeec0bc",
      "./target/2ae9fd3f-4c70-4d9f-9fe0-98cb2f0b7521",
      "./target/fbb52038-4db1-46d3-a4de-108fd12cbfc7",
      "./target/b15449b6-14c9-406b-bce9-749805dd6a3e",
      "./target/5deacf2d-833a-4549-a398-20a0abeec0bt",
    )

  override def afterAll(): Unit = {
    super.afterAll()
    partitionedDirs
      .flatMap(dir => new File(dir).listFiles().toSeq :+ new File(dir))
      .foreach { file =>
        if (Files.exists(Paths.get(file.getAbsolutePath))) {
          Files.delete(Paths.get(file.getAbsolutePath))
        }
      }
  }

  trait DataWriterSpecData {
    implicit val actorSystem: ActorSystem = ActorSystem.create("DataWriterSpec")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val dataWriter = new DataWriter(2, 1)
    val dirPath = "./target"

    val duration = Duration(20, TimeUnit.SECONDS)

    def await[T](future: Future[T]): T = {
      Await.result(future, duration)
    }
  }

  "DataWriter" should {
    "write partitioned impressions by app_id and country_code" in new DataWriterSpecData {
      val impressionsJson: String =
        SourceIO
          .fromFile("./src/test/resources/impressions.json")
          .mkString

      val impressionsSource: Source[Impression, NotUsed] = Source.fromIterator(() => Json.parse(impressionsJson).as[Iterator[Impression]])

      val result: Future[Done] =
        dataWriter
          .writePartitions(impressionsSource, Paths.get(dirPath))(impression => s"${impression.app_id}_${impression.country_code.getOrElse("NONE")}")
          .runWith(Sink.ignore)

      await(result)

      Files.exists(Paths.get(s"$dirPath/32_UK")) shouldBe true
      Files.exists(Paths.get(s"$dirPath/30_NONE")) shouldBe true
      Files.exists(Paths.get(s"$dirPath/4_IT")) shouldBe true
      Files.exists(Paths.get(s"$dirPath/22_IT")) shouldBe true
      Files.exists(Paths.get(s"$dirPath/9_")) shouldBe true

      new File(s"$dirPath/32_UK").listFiles().length shouldBe 3
      new File(s"$dirPath/30_NONE").listFiles().length shouldBe 3
      new File(s"$dirPath/4_IT").listFiles().length shouldBe 2
      new File(s"$dirPath/22_IT").listFiles().length shouldBe 2
      new File(s"$dirPath/9_").listFiles().length shouldBe 2
    }

    "write partitioned clicks by impression_id" in new DataWriterSpecData {
      val clicksJson: String =
        SourceIO
          .fromFile("./src/test/resources/clicks.json")
          .mkString

      val clicksSource: Source[Click, NotUsed] = Source.fromIterator(() => Json.parse(clicksJson).as[Iterator[Click]])

      val result: Future[Done] =
        dataWriter
          .writePartitions(clicksSource, Paths.get(dirPath))(click => click.impression_id)
          .runWith(Sink.ignore)

      await(result)

      Files.exists(Paths.get(s"$dirPath/a39747e8-9c58-41db-8f9f-27963bc248b5")) shouldBe true
      Files.exists(Paths.get(s"$dirPath/a39747e8-9c58-41db-8f9f-27963bc248b6")) shouldBe true
      Files.exists(Paths.get(s"$dirPath/a39747e8-9c58-41db-8f9f-27963bc248b7")) shouldBe true
      Files.exists(Paths.get(s"$dirPath/5deacf2d-833a-4549-a398-20a0abeec0bc")) shouldBe true
      Files.exists(Paths.get(s"$dirPath/2ae9fd3f-4c70-4d9f-9fe0-98cb2f0b7521")) shouldBe true
      Files.exists(Paths.get(s"$dirPath/fbb52038-4db1-46d3-a4de-108fd12cbfc7")) shouldBe true
      Files.exists(Paths.get(s"$dirPath/b15449b6-14c9-406b-bce9-749805dd6a3e")) shouldBe true
      Files.exists(Paths.get(s"$dirPath/5deacf2d-833a-4549-a398-20a0abeec0bt")) shouldBe true

      new File(s"$dirPath/a39747e8-9c58-41db-8f9f-27963bc248b5").listFiles().length shouldBe 1
      new File(s"$dirPath/a39747e8-9c58-41db-8f9f-27963bc248b6").listFiles().length shouldBe 1
      new File(s"$dirPath/a39747e8-9c58-41db-8f9f-27963bc248b7").listFiles().length shouldBe 1
      new File(s"$dirPath/5deacf2d-833a-4549-a398-20a0abeec0bc").listFiles().length shouldBe 1
      new File(s"$dirPath/2ae9fd3f-4c70-4d9f-9fe0-98cb2f0b7521").listFiles().length shouldBe 1
      new File(s"$dirPath/fbb52038-4db1-46d3-a4de-108fd12cbfc7").listFiles().length shouldBe 1
      new File(s"$dirPath/b15449b6-14c9-406b-bce9-749805dd6a3e").listFiles().length shouldBe 1
      new File(s"$dirPath/5deacf2d-833a-4549-a398-20a0abeec0bt").listFiles().length shouldBe 1
    }
  }

}
