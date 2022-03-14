import sbt._

object Versions {
  val Kafka = "2.8.0"
  val Circe = "0.14.1"
  val ScalaTest = "3.2.11"
}

object Dependencies {
  private val kafka = Seq(
    "org.apache.kafka"  % "kafka-clients"       % Versions.Kafka,
    "org.apache.kafka"  % "kafka-streams"       % Versions.Kafka,
    "org.apache.kafka" %% "kafka-streams-scala" % Versions.Kafka
  )

  private val circe = Seq(
    "io.circe" %% "circe-core"    % Versions.Circe,
    "io.circe" %% "circe-generic" % Versions.Circe,
    "io.circe" %% "circe-parser"  % Versions.Circe
  )

  private val scalaTest = Seq("org.scalatest" %% "scalatest" % Versions.ScalaTest)

  val appDependencies = scalaTest ++ kafka ++ circe
}
