name := "HelloSparkScala"
version := "1.0"
scalaVersion := "2.11.8"

val localArtifactoryRelease       = "local-artifactory-release"        at "http://windows10.server.com:9081/artifactory/libs-release"

libraryDependencies ++= Seq(
	"org.apache.spark" % "spark-core_2.11" % "2.1.0" % "provided",
	"org.apache.spark" % "spark-sql_2.11" % "2.1.0" % "provided" 
)