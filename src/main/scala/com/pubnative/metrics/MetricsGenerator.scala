package com.pubnative.metrics

import java.io.File
import java.nio.file.Path

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.pubnative.data.loader.DataLoader
import com.pubnative.domain.Click

import scala.concurrent.{ExecutionContext, Future}

private[metrics] case class ImpressionIdsWithMetric(seenImpressionIds: Set[String], metric: Metric)

class MetricsGenerator(dataLoader: DataLoader)(implicit materializer: Materializer, ec: ExecutionContext) {

  def generateMetrics(partitionedImpressionsDir: Path,
                      partitionedClicksDir: Path): Source[Metric, NotUsed] = {

    Source
      .fromIterator(() => partitionedImpressionsDir.toFile.listFiles().toIterator)
      .flatMapConcat(partitionedDir => generateMetricsForPartition(partitionedDir, partitionedClicksDir))
  }

  private[metrics] def generateMetricsForPartition(partitionedDir: File, partitionedClicksDir: Path): Source[Metric, NotUsed] = {
    dataLoader
      .loadImpressions(partitionedDir.listFiles().map(_.toPath).toIterator)
      .foldAsync(ImpressionIdsWithMetric(Set.empty[String], Metric.empty)) { (acc, impression) =>
        for {
          eventualClicks: List[Click] <- {
            if (acc.seenImpressionIds.contains(impression.id)) Future.successful(List.empty[Click])
            else readClicksByImpressionId(impression.id, partitionedClicksDir)
          }
          metric <- {
            Future.successful(
              Metric(
                Some(impression.app_id),
                impression.country_code,
                1,
                eventualClicks.size,
                eventualClicks.foldLeft(0.0)((acc, click) => acc + click.revenue)
              )
            )
          }
        } yield ImpressionIdsWithMetric(acc.seenImpressionIds + impression.id, Metric.combine(acc.metric, metric))
      }
      .map(_.metric)
  }

  private[metrics] def readClicksByImpressionId(impressionId: String, partitionedClicksDir: Path) = {
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
