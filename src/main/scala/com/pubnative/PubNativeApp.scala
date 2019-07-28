package com.pubnative

import java.util.concurrent.Executors

import com.typesafe.scalalogging.Logger
import org.rogach.scallop._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

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
  val logger = Logger("PubNativeApp")

  def main(args: Array[String]): Unit = {
    val conf = new PubNativeConf(args)

    val numberOfCores = Runtime.getRuntime.availableProcessors()
    val numberOfThreads: Int = (numberOfCores * 2) - 2
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(numberOfThreads))

    val pubNativeBusinessLogic = new PubNativeBusinessLogic(conf, numberOfThreads)
    pubNativeBusinessLogic
      .generateMetricsAndTopAdvertisers()
      .map(_ => System.exit(0))
      .recover {
        case e: Throwable =>
          logger.error("error while generating metrics and top advertisers", e)
          System.exit(1)
      }
  }
}
