name := "HelloSparkScala"
version := "1.0"
scalaVersion := "2.11.8"

val localArtifactoryRelease       = "local-artifactory-release"        at "http://windows10.server.com:9081/artifactory/libs-release"

libraryDependencies ++= Seq(
	"com.databricks" % "spark-avro_2.11" % "3.2.0" % "provided",
	"org.apache.spark" % "spark-core_2.11" % "2.1.0" % "provided",
	"org.apache.spark" % "spark-sql_2.11" % "2.1.0" % "provided",
	"org.apache.spark" % "spark-streaming_2.11" % "2.1.0" % "provided",
	"org.apache.spark" % "spark-streaming-kafka_2.11" % "1.6.1",
	"com.github.nscala-time" % "nscala-time_2.11" % "2.16.0"
)