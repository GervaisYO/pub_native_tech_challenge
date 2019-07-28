package com.pubnative.recommendations

import java.io.File
import java.nio.file.Path

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.pubnative.data.loader.DataLoader
import com.pubnative.data.writer.DataWriter
import com.pubnative.domain.Click

import scala.concurrent.{ExecutionContext, Future}

private[recommendations] case class ImpressionIdsWithARIR(seenImpressionIds: Set[String],
                                                          advertiserRevenueImpressionRate: AdvertiserRevenueImpressionRate)

private[recommendations] object AdvertiserRevenueImpressionRate {
  val empty = AdvertiserRevenueImpressionRate(None, None, None, 0.0, 0)

  def combine(advertiserRevenueImpressionRate1: AdvertiserRevenueImpressionRate,
              advertiserRevenueImpressionRate2: AdvertiserRevenueImpressionRate): AdvertiserRevenueImpressionRate = {
    AdvertiserRevenueImpressionRate(
      advertiserRevenueImpressionRate2.app_id,
      advertiserRevenueImpressionRate2.country_code,
      advertiserRevenueImpressionRate2.advertiserId,
      advertiserRevenueImpressionRate2.revenue + advertiserRevenueImpressionRate1.revenue,
      advertiserRevenueImpressionRate2.impressionCount + advertiserRevenueImpressionRate1.impressionCount
    )
  }
}

private[recommendations] case class AdvertiserRevenueImpressionRate(app_id: Option[Int],
                                                                    country_code: Option[String],
                                                                    advertiserId: Option[Int],
                                                                    revenue: Double,
                                                                    impressionCount: Int)

class RecommendationGenerator(dataLoader: DataLoader, dataWriter: DataWriter, topAdvertiserCount: Int)
                             (implicit materializer: Materializer, ec: ExecutionContext) {

  def generateRecommendations(partitionedImpressionsDir: Path,
                              partitionedClicksDir: Path): Source[Recommendation, NotUsed] = {

    Source
      .fromIterator(() => partitionedImpressionsDir.toFile.listFiles().toIterator)
      .flatMapConcat(partitionedDir => generateRecommendationsForPartition(partitionedDir, partitionedClicksDir))
  }

  private[recommendations] def generateRecommendationsForPartition(partitionedDir: File,
                                                                   partitionedClicksDir: Path) = {
    generateAIRForPartition(partitionedDir, partitionedClicksDir)
      .fold(List.empty[AdvertiserRevenueImpressionRate]) { (acc, advertiserRevenueImpressionRate) =>
        advertiserRevenueImpressionRate :: acc
      }
      .map(toRecommendation)
  }

  private[recommendations] def toRecommendation(advertiserRevenueImpressionRates: List[AdvertiserRevenueImpressionRate]): Recommendation = {
    val topAdvertisers =
      advertiserRevenueImpressionRates
      .sortBy(advertiserRevenueImpressionRate => -(advertiserRevenueImpressionRate.revenue / advertiserRevenueImpressionRate.impressionCount))
      .take(topAdvertiserCount)

    val topAdvertiser = topAdvertisers.head

    Recommendation(topAdvertiser.app_id, topAdvertiser.country_code, topAdvertisers.map(_.advertiserId.get))
  }

  private[recommendations] def generateAIRForPartition(partitionedDir: File,
                                                       partitionedClicksDir: Path): Source[AdvertiserRevenueImpressionRate, NotUsed] = {
    dataLoader
      .loadImpressions(partitionedDir.listFiles().map(_.toPath).toIterator)
      .groupBy(Int.MaxValue, impression => impression.advertiser_id)
      .foldAsync(ImpressionIdsWithARIR(Set.empty[String], AdvertiserRevenueImpressionRate.empty)) { (acc, impression) =>
        for {
          eventualClicks: List[Click] <- {
            if (acc.seenImpressionIds.contains(impression.id)) Future.successful(List.empty[Click])
            else readClicksByImpressionId(impression.id, partitionedClicksDir)
          }
          advertiserRevenueImpressionRate <- {
            Future.successful(
              AdvertiserRevenueImpressionRate(
                Some(impression.app_id),
                impression.country_code,
                Some(impression.advertiser_id),
                eventualClicks.foldLeft(0.0)((acc, click) => acc + click.revenue),
                1
              )
            )
          }
        } yield {
          ImpressionIdsWithARIR(
            acc.seenImpressionIds + impression.id,
            AdvertiserRevenueImpressionRate.combine(acc.advertiserRevenueImpressionRate, advertiserRevenueImpressionRate))
        }
      }
      .filter { impressionWithRecommendation =>
        impressionWithRecommendation.advertiserRevenueImpressionRate.advertiserId.isDefined
      }
      .map(_.advertiserRevenueImpressionRate)
      .mergeSubstreams
  }

  private[recommendations] def readClicksByImpressionId(impressionId: String, partitionedClicksDir: Path) = {
    val clicksForImpressions: Array[Path] = for {
      impressionIdClicksDir <- partitionedClicksDir.toFile.listFiles().filter(_.getName.startsWith(impressionId))
      impressionIdPaths <- impressionIdClicksDir.listFiles().map(_.toPath)
    } yield impressionIdPaths

    dataLoader
      .loadClicks(clicksForImpressions.toIterator)
      .runFold(List.empty[Click]) { (acc, click) =>
        click :: acc
      }
  }

}
