package com.pubnative

import java.util.concurrent.Executors

import com.typesafe.scalalogging.Logger
import org.rogach.scallop._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try

class PubNativeConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val impressionsDir: ScallopOption[String] = opt[String]("impressions-dir", required = true)
  val clicksDir: ScallopOption[String] = opt[String]("clicks-dir", required = true)
  val outputDir: ScallopOption[String] = opt[String]("output-dir", required = true)
  val topAdvertiserCount: ScallopOption[Int] = opt[Int]("top-advertiser-count")
  val streamsParallelism: ScallopOption[Int] = opt[Int]("streams-parallelism")
  val sourceGroupSize: ScallopOption[Int] = opt[Int]("source-group-size")
  verify()
}

object PubNativeApp {
  val logger = Logger("PubNativeApp")

  def main(args: Array[String]): Unit = {
    val conf = new PubNativeConf(args)

    val numberOfCores = Runtime.getRuntime.availableProcessors()
    val numberOfThreads: Int = (numberOfCores * 2) - 2
    val executorService = Executors.newFixedThreadPool(numberOfThreads)
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(executorService)

    val pubNativeBusinessLogic = new PubNativeBusinessLogic(conf, numberOfThreads)

    (for {
      _ <- pubNativeBusinessLogic.generateMetricsAndTopAdvertisers()
      _ <- pubNativeBusinessLogic.actorSystem.terminate()
      _ <- Future.fromTry(Try(System.exit(0)))
    } yield ())
      .recover {
        case e: Throwable =>
          logger.error("error while generating metrics and top advertisers", e)
          System.exit(1)
      }
  }
}
