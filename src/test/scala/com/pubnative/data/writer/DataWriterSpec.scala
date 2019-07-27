package com.pubnative.data.writer

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.pubnative.domain.Impression
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
    )

  override def afterAll(): Unit = {
    super.afterAll()
    partitionedDirs
      .flatMap(dir => new File(dir).listFiles().toSeq :+ new File(dir))
      .foreach(file => Files.delete(Paths.get(file.getAbsolutePath)))
  }

  trait DataWriterSpecData {
    implicit val actorSystem: ActorSystem = ActorSystem.create("DataWriterSpec")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val dataWriter = new DataWriter(2, 1, "./target")

    val duration = Duration(20, TimeUnit.SECONDS)

    def await[T](future: Future[T]): T = {
      Await.result(future, duration)
    }
  }

  "DataWriter" should {
    "write partitioned data" in new DataWriterSpecData {
      val impressionsJson: String =
        SourceIO
          .fromFile("./src/test/resources/impressions.json")
          .mkString

      val impressionsSource = Source.fromIterator(() => Json.parse(impressionsJson).as[Iterator[Impression]])

      val result =
        dataWriter
          .writePartitions(impressionsSource)(impression => s"${impression.app_id}_${impression.country_code.getOrElse("NONE")}")
          .runWith(Sink.ignore)

      await(result)

      Files.exists(Paths.get(s"${dataWriter.directoryPath}/32_UK")) shouldBe true
      Files.exists(Paths.get(s"${dataWriter.directoryPath}/30_NONE")) shouldBe true
      Files.exists(Paths.get(s"${dataWriter.directoryPath}/4_IT")) shouldBe true
      Files.exists(Paths.get(s"${dataWriter.directoryPath}/22_IT")) shouldBe true
      Files.exists(Paths.get(s"${dataWriter.directoryPath}/9_")) shouldBe true

      new File(s"${dataWriter.directoryPath}/32_UK").listFiles().length shouldBe 2
      new File(s"${dataWriter.directoryPath}/30_NONE").listFiles().length shouldBe 1
      new File(s"${dataWriter.directoryPath}/4_IT").listFiles().length shouldBe 1
      new File(s"${dataWriter.directoryPath}/22_IT").listFiles().length shouldBe 1
      new File(s"${dataWriter.directoryPath}/9_").listFiles().length shouldBe 1
    }
  }

}
