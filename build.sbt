name := "PubNative"

version := "0.1"

scalaVersion := "2.12.8"

parallelExecution in Test := false

mainClass in assembly := Some("com.pubnative.PubNativeApp")
assemblyJarName in assembly := "pub_native_tech_challenge.jar"
assemblyOutputPath in assembly := new File("./pub-native-jar/pub_native_tech_challenge.jar")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.23",
  "com.typesafe.play" %% "play-json" % "2.7.4",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.typelevel" %% "cats-core" % "1.6.1",
  "org.rogach" %% "scallop" % "3.3.1",
  "org.slf4j" % "slf4j-api" % "1.7.26",
  "org.slf4j" % "slf4j-simple" % "1.7.26",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)