package com.pubnative

import java.io.File
import java.nio.file.Paths
import java.time.Instant
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.pubnative.data.loader.DataLoader
import com.pubnative.data.writer.DataWriter
import com.pubnative.metrics.MetricsGenerator
import com.pubnative.recommendations.RecommendationGenerator
import com.typesafe.scalalogging.Logger
import org.rogach.scallop._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

class PubNativeConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val impressionsDir: ScallopOption[String] = opt[String](required = true)
  val clicksDir: ScallopOption[String] = opt[String](required = true)
  val outputDir: ScallopOption[String] = opt[String](required = true)
  val topAdvertiserCount: ScallopOption[Int] = opt[Int]()
  val streamsParallelism: ScallopOption[Int] = opt[Int]()
  val sourceGroupSize: ScallopOption[Int] = opt[Int]()
  verify()
}

object PubNativeApp {
  def main(args: Array[String]): Unit = {
    val conf = new PubNativeConf(args)
    val pubNativeBusinessLogic = new PubNativeBusinessLogic(conf)
    Await.result(pubNativeBusinessLogic.generateMetricsAndToAdvertisers(), Duration.Inf)
  }
}

class PubNativeBusinessLogic(pubNativeConf: PubNativeConf) {

  import pubNativeConf._

  val logger = Logger("PubNativeBusinessLogic")

  private val numberOfCores = Runtime.getRuntime.availableProcessors()
  private val numberOfThreads: Int = (numberOfCores * 2) - 2
  private implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(numberOfThreads))
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

  private val partitionedImpressionDir = Paths.get(outputDirectory, s"impressions_${Instant.now().toEpochMilli}")
  private val partitionedClickDir = Paths.get(outputDirectory, s"clicks_${Instant.now().toEpochMilli}")
  private val metricsFilePath = Paths.get(outputDirectory, "metrics")
  private val recommendationsFilePath = Paths.get(outputDirectory, "recommendations")

  def generateMetricsAndToAdvertisers(): Future[Unit] = {
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
      _ <- Future.successful(logger.info("impressions partitioned generated"))
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
      _ <- Future.successful(logger.info("click partitioned generated"))
    } yield writeResult
  }

}
