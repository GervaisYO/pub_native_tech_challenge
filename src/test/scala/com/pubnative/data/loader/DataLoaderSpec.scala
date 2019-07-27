package com.pubnative.data.loader

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.pubnative.domain.{Click, Impression}
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source


class DataLoaderSpec extends WordSpec with Matchers {

  trait DataLoadSpecData {
    val dataLoader = new DataLoader(1)
    val impressionJsonContent: String = Source.fromResource("impressions.json").mkString
    val clickJsonContent: String = Source.fromResource("clicks.json").mkString

    implicit val actorSystem: ActorSystem = ActorSystem.create("DataLoaderSpec")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val duration = Duration(5, TimeUnit.SECONDS)

    def await[T](future: Future[T]): T = {
      Await.result(future, duration)
    }
  }

  "DataLoader" should {
    "read impressions when given file paths"  in new DataLoadSpecData {
      val impressions =
        dataLoader
          .loadImpressions(List("./src/test/resources/impressions.json").toIterator)
          .runFold(List.empty[Impression]) { (acc, impression) =>
            impression :: acc
          }

      await(impressions) should contain theSameElementsAs List(
        Impression("a39747e8-9c58-41db-8f9f-27963bc248b5", 32, Some("UK"), 8),
        Impression("5deacf2d-833a-4549-a398-20a0abeec0bc", 30, None, 17),
        Impression("2ae9fd3f-4c70-4d9f-9fe0-98cb2f0b7521", 4, Some("IT"), 15),
        Impression("fbb52038-4db1-46d3-a4de-108fd12cbfc7", 22, Some("IT"), 20),
        Impression("b15449b6-14c9-406b-bce9-749805dd6a3e", 9, Some(""), 32)
      )
    }

    "convert impressions json to iterator of impressions" in new DataLoadSpecData {
      val impressionSource = dataLoader.convertJsonStringToDomainObject[Impression](impressionJsonContent)
      val impressions =
        impressionSource
          .runFold(List.empty[Impression]) { (acc, impression) =>
            impression :: acc
          }

      await(impressions).size shouldBe 5
    }

    "read click when given file paths"  in new DataLoadSpecData {
      val clicks =
        dataLoader
          .loadClicks(List("./src/test/resources/clicks.json").toIterator)
          .runFold(List.empty[Click]) { (acc, click) =>
            click :: acc
          }

      await(clicks) should contain theSameElementsAs List(
        Click("97dd2a0f-6d42-4c63-8cd6-5270c19f20d6", 2.091225600111518),
        Click("43bd7feb-3fea-40b4-a140-d01a35ec1f73", 2.4794577548980876),
        Click("1b04c706-e3d7-4f70-aac8-25635fa24250", 1.0617394700223026),
        Click("31214d91-d950-4464-8316-ed3d302d49d3", 2.074762244925742),
        Click("fbb52038-4db1-46d3-a4de-108fd12cbfc7", 2.7837584244414537)
      )
    }

    "convert clicks json to iterator of clicks" in new DataLoadSpecData {
      val clickSource = dataLoader.convertJsonStringToDomainObject[Click](clickJsonContent)
      val clicks =
        clickSource
          .runFold(List.empty[Click]) { (acc, click) =>
            click :: acc
          }

      await(clicks).size shouldBe 5
    }
  }
}
