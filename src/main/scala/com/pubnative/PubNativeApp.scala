package com.pubnative

import org.rogach.scallop._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

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
    Await.result(pubNativeBusinessLogic.generateMetricsAndTopAdvertisers(), Duration.Inf)
  }
}
