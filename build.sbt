name := "HelloSparkScala"
version := "1.0"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
	"org.apache.spark" % "spark-core_2.10" % "2.1.0" % "provided"
)