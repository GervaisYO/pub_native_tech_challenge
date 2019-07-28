package com.pubnative.data.writer

import java.io.{File, PrintWriter}
import java.nio.file.{FileAlreadyExistsException, Files, Path, Paths}
import java.time.Instant
import java.util.UUID

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import cats.data.EitherT
import com.pubnative.data.loader.DataLoader
import com.typesafe.scalalogging.Logger
import play.api.libs.json.{Json, Writes}
import cats.implicits._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class DataWriter(parallelism: Int, groupSize: Int)
                (implicit ec: ExecutionContext, materializer: Materializer) {

  private val logger = Logger(classOf[DataLoader])

  def writePartitions[T](source: Source[T, NotUsed], directoryPath: Path)(partitionFunction: T => String)
                        (implicit fjs: Writes[T]): Source[Int, NotUsed] = {
    source
      .grouped(groupSize)
      .mapAsync(parallelism) { elements =>
        val groupedElements = elements.groupBy(partitionFunction)
        Future.sequence(
          groupedElements.map {
            case (key, elementsForKey) =>
              EitherT(writeElementsToFile(key, elementsForKey, directoryPath))
              .leftMap(throwable => logger.error(s"error while writing elements for key $key", throwable))
              .value
          }
        )
        .map(iterable => iterable.size)
      }
  }

  private[data] def writeElementsToFile[T](key: String, elements: Seq[T], directoryPath: Path)
                                          (implicit fjs: Writes[T]): Future[Either[Throwable, Unit]] = {
    val pathDir = Paths.get(s"${directoryPath.toFile.getAbsolutePath}/$key")
    val filePath = s"${directoryPath.toFile.getAbsolutePath}/$key/${key}_${UUID.randomUUID()}_${Instant.now().toEpochMilli.toString}.json"
    if (Files.exists(pathDir)) {
      writeToFile(
        Paths.get(filePath),
        elements.map(element => Json.toJson(element).toString()).mkString("[", ",", "]")
      )
    }
    else {
      (for {
        _ <- EitherT(createDirectory(pathDir))
            .recover {
              case _: FileAlreadyExistsException => pathDir
            }
        writeResult <- EitherT {
          writeToFile(
            Paths.get(filePath),
            elements.map(element => Json.toJson(element).toString()).mkString("[", ",", "]")
          )
        }
      } yield writeResult)
      .value
    }
  }

  private[data] def writeToFile(filePath: Path, content: String): Future[Either[Throwable, Unit]] = Future {
    Try {
      val pw = new PrintWriter(new File(filePath.toFile.getAbsolutePath))
      pw.write(content)
      pw.flush()
      pw.close()
    }.toEither
  }

  private[data] def createDirectory(dir: Path): Future[Either[Throwable, Path]] = {
    Future(Try(Files.createDirectories(dir)).toEither)
  }
}
