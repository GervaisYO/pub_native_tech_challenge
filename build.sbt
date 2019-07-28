name := "PubNative"

version := "0.1"

scalaVersion := "2.12.8"

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.23",
  "com.typesafe.play" %% "play-json" % "2.7.4",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.typelevel" %% "cats-core" % "1.6.1",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)