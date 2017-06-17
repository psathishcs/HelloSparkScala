package org.hello
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.scalatest.time.Second

object HelloSparkScream {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Hello Spark Screaming = Scala NetWork Word Count")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("hadoop.master.com", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_+_)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }
}