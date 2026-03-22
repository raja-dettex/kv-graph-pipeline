name := "kv-graph-spark"

version := "0.1"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.httpcomponents.client5" % "httpclient5" % "5.2.1",
  "org.apache.httpcomponents.client5" % "httpclient5-fluent" % "5.2.1"
)