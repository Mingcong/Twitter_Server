name := "twitterserver"

version := "1.0"

scalaVersion := "2.11.4"

resolvers += "spray repo" at "http://repo.spray.io"

val sprayVersion = "1.3.2"

libraryDependencies ++= Seq(
  "io.spray" %%  "spray-json" % "1.3.0",
  "com.typesafe.akka" %% "akka-actor" % "2.3.6",
  "com.typesafe.akka" %% "akka-http-experimental" % "0.7",
  "io.spray" %% "spray-routing" % sprayVersion,
  "io.spray" %% "spray-client" % sprayVersion,
  "io.spray" %% "spray-testkit" % sprayVersion % "test",
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "org.scalatest" %% "scalatest" % "2.2.2" % "test",
  "org.mockito" % "mockito-all" % "1.9.5" % "test"
)
