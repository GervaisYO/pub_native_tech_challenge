package com.pubnative.data.loader

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.pubnative.domain.{Click, Impression}
import com.typesafe.scalalogging.Logger
import play.api.libs.json.{Json, Reads}

import scala.concurrent.{ExecutionContext, Future}
import scala.io.{Source => SourceIO}
import scala.util.Try

case class ThrowableWithPath(t: Throwable, path: String)

class DataLoader(parallelism: Int)(implicit ec: ExecutionContext) {

  private val logger = Logger(classOf[DataLoader])

  def loadImpressions(filePaths: Iterator[String]): Source[Impression, NotUsed] = {
    readDomainObjectFromPath[Impression](filePaths)
  }

  def loadClicks(filePaths: Iterator[String]): Source[Click, NotUsed] = {
    readDomainObjectFromPath[Click](filePaths)
  }

  private[data] def readDomainObjectFromPath[T](filePaths: Iterator[String])
                                               (implicit fjs: Reads[T]): Source[T, NotUsed] = {
    Source
      .fromIterator(() => filePaths)
      .mapAsync(parallelism)(readDomainObjectFromPath)
      .flatMapConcat {
        case Right(value) => convertJsonStringToDomainObject(value)
        case Left(throwableWithPath) =>
          logger.error(
            s"error while reading domain objects from path ${throwableWithPath.path}",
            throwableWithPath.t
          )
          Source.empty[T]
      }
  }

  private[data] def readDomainObjectFromPath(path: String): Future[Either[ThrowableWithPath, String]] = {
    Future {
      Try(SourceIO.fromFile(path).mkString)
        .toEither
        .swap
        .map(t => ThrowableWithPath(t, path))
        .swap
    }
  }

  private[data] def convertJsonStringToDomainObject[T](jsonString: String)
                                                      (implicit fjs: Reads[T]): Source[T, NotUsed] = {
    Source.fromIterator(() => Json.parse(jsonString).as[Iterator[T]])
  }

}
