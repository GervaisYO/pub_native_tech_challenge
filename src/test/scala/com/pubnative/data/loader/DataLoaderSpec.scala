package com.pubnative.data.loader

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, scaladsl}
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
      val impressions: Future[List[Impression]] =
        dataLoader
          .loadImpressions(List(Paths.get("./src/test/resources/impressions.json")).toIterator)
          .runFold(List.empty[Impression]) { (acc, impression) =>
            impression :: acc
          }

      await(impressions) should contain theSameElementsAs List(
        Impression("b15449b6-14c9-406b-bce9-749805dd6a32", 9, Some(""), 12),
        Impression("b15449b6-14c9-406b-bce9-749805dd6a3e", 9, Some(""), 11),
        Impression("fbb52038-4db1-46d3-a4de-108fd12cbfc8", 22, Some("IT"), 10),
        Impression("fbb52038-4db1-46d3-a4de-108fd12cbfc7", 22, Some("IT"), 9),
        Impression("2ae9fd3f-4c70-4d9f-9fe0-98cb2f0b7522", 4, Some("IT"), 8),
        Impression("2ae9fd3f-4c70-4d9f-9fe0-98cb2f0b7521", 4, Some("IT"), 7),
        Impression("5deacf2d-833a-4549-a398-20a0abeec0bs", 30, None, 6),
        Impression("5deacf2d-833a-4549-a398-20a0abeec0bt", 30, None, 5),
        Impression("5deacf2d-833a-4549-a398-20a0abeec0bc", 30, None, 4),
        Impression("a39747e8-9c58-41db-8f9f-27963bc248b7", 32, Some("UK"), 3),
        Impression("a39747e8-9c58-41db-8f9f-27963bc248b6", 32, Some("UK"), 2),
        Impression("a39747e8-9c58-41db-8f9f-27963bc248b5", 32, Some("UK"), 1)
      )
    }

    "convert impressions json to iterator of impressions" in new DataLoadSpecData {
      val impressionSource: scaladsl.Source[Impression, NotUsed] =
        dataLoader.convertJsonStringToDomainObject[Impression](impressionJsonContent)
      val impressions: Future[List[Impression]] =
        impressionSource
          .runFold(List.empty[Impression]) { (acc, impression) =>
            impression :: acc
          }

      await(impressions).size shouldBe 12
    }

    "read click when given file paths"  in new DataLoadSpecData {
      val clicks: Future[List[Click]] =
        dataLoader
          .loadClicks(List(Paths.get("./src/test/resources/clicks.json")).toIterator)
          .runFold(List.empty[Click]) { (acc, click) =>
            click :: acc
          }

      await(clicks) should contain theSameElementsAs List(
        Click("b15449b6-14c9-406b-bce9-749805dd6a3e", 0.783758424441452),
        Click("fbb52038-4db1-46d3-a4de-108fd12cbfc7", 2.783758424441452),
        Click("2ae9fd3f-4c70-4d9f-9fe0-98cb2f0b7521", 2.7837584244414537),
        Click("5deacf2d-833a-4549-a398-20a0abeec0bc", 2.074762244925742),
        Click("a39747e8-9c58-41db-8f9f-27963bc248b7", 1.0617394700223026),
        Click("a39747e8-9c58-41db-8f9f-27963bc248b6", 2.4794577548980876),
        Click("a39747e8-9c58-41db-8f9f-27963bc248b5", 2.091225600111518),
        Click("5deacf2d-833a-4549-a398-20a0abeec0bt", 0.583758424441452)
      )
    }

    "convert clicks json to iterator of clicks" in new DataLoadSpecData {
      val clickSource: scaladsl.Source[Click, NotUsed] =
        dataLoader.convertJsonStringToDomainObject[Click](clickJsonContent)
      val clicks: Future[List[Click]] =
        clickSource
          .runFold(List.empty[Click]) { (acc, click) =>
            click :: acc
          }

      await(clicks).size shouldBe 8
    }
  }
}
