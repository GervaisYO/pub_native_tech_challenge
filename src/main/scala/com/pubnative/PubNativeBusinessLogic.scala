package com.pubnative

import java.io.File
import java.nio.file.Paths
import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.pubnative.data.loader.DataLoader
import com.pubnative.data.writer.DataWriter
import com.pubnative.metrics.MetricsGenerator
import com.pubnative.recommendations.RecommendationGenerator
import com.typesafe.scalalogging.Logger

import scala.concurrent.{ExecutionContext, Future}

class PubNativeBusinessLogic(pubNativeConf: PubNativeConf, numberOfThreads: Int)(implicit ec: ExecutionContext) {

  import pubNativeConf._

  val logger = Logger(classOf[PubNativeBusinessLogic])

  implicit val actorSystem: ActorSystem = ActorSystem.apply(name = "PubNativeBusinessLogic", defaultExecutionContext = Some(ec))
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  private val parallelism: Int = streamsParallelism.getOrElse(numberOfThreads)
  private val sourceGroupingSize = sourceGroupSize.getOrElse(1000)
  private val impressionDirectory = impressionsDir.toOption.get
  private val clickDirectory = clicksDir.toOption.get
  private val outputDirectory = outputDir.toOption.get
  private val dataLoader = new DataLoader(parallelism)
  private val dataWriter = new DataWriter(parallelism, sourceGroupingSize)
  private val metricsGenerator = new MetricsGenerator(dataLoader)
  private val recommendationGenerator = new RecommendationGenerator(dataLoader, dataWriter, topAdvertiserCount.getOrElse(5))

  private val partitionedImpressionDir = Paths.get(outputDirectory, s"partitioned_impressions_${Instant.now().toEpochMilli}")
  private val partitionedClickDir = Paths.get(outputDirectory, s"partitioned_clicks_${Instant.now().toEpochMilli}")
  private val metricsFilePath = Paths.get(outputDirectory, "metrics.json")
  private val recommendationsFilePath = Paths.get(outputDirectory, "recommendations.json")

  def generateMetricsAndTopAdvertisers(): Future[Unit] = {
    for {
      _ <- writePartitionedImpressions()
      _ <- writePartitionedClicks()
      _ <- generateMetrics()
      _ <- generateRecommendation()
    } yield ()
  }

  private def generateMetrics() = {
    for {
      _ <- Future.successful(logger.info("generating metrics"))
      metricSource <- Future(metricsGenerator.generateMetrics(partitionedImpressionDir, partitionedClickDir))
      contentSource <- Future.successful(dataWriter.writeContentToFile(metricSource, metricsFilePath))
      writeResult <- contentSource.runWith(Sink.ignore)
      _ <- Future.successful(logger.info(s"metrics generated and are stored in file ${metricsFilePath.toFile.getAbsolutePath}"))
    } yield writeResult
  }

  private def generateRecommendation() = {
    for {
      _ <- Future.successful(logger.info("generating recommendations"))
      recommendationSource <- Future(recommendationGenerator.generateRecommendations(partitionedImpressionDir, partitionedClickDir))
      contentSource <- Future.successful(dataWriter.writeContentToFile(recommendationSource, recommendationsFilePath))
      writeResult <- contentSource.runWith(Sink.ignore)
      _ <- Future.successful(logger.info(s"recommendations generated and are stored in file ${recommendationsFilePath.toFile.getAbsolutePath}"))
    } yield writeResult
  }

  private def writePartitionedImpressions() = {
    for {
      _ <- Future.successful(logger.info("generating partitioned impressions"))
      impressionFileIterator <- Future(new File(impressionDirectory).listFiles().map(_.toPath).toIterator)
      impressionSource <- Future(dataLoader.loadImpressions(impressionFileIterator))
      partitionedImpressionSource <- Future(
        dataWriter
          .writePartitions(impressionSource, partitionedImpressionDir)
          (impression => s"${impression.app_id}_${impression.country_code.getOrElse("NONE")}")
      )
      writeResult <- partitionedImpressionSource.runWith(Sink.ignore)
      _ <- Future.successful(logger.info("partitioned impressions generated"))
    } yield writeResult
  }

  private def writePartitionedClicks() = {
    for {
      _ <- Future.successful(logger.info("generating partitioned clicks"))
      clickFileIterator <- Future(new File(clickDirectory).listFiles().map(_.toPath).toIterator)
      clickSource <- Future(dataLoader.loadClicks(clickFileIterator))
      partitionedClickSource <- Future(
        dataWriter
          .writePartitions(clickSource, partitionedClickDir)
          (click => click.impression_id)
      )
      writeResult <- partitionedClickSource.runWith(Sink.ignore)
      _ <- Future.successful(logger.info("partitioned clicks generated"))
    } yield writeResult
  }

}
