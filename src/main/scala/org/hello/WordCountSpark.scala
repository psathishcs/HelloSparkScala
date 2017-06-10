package org.hello

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object WordCountSpark {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("WordCount-Scala")
    val spark = new SparkContext(conf);
    val fsConfig = new Configuration()
    fsConfig.set("fs.defaultFS", "hdfs://hadoop.master.com:9000");
    val fs = FileSystem.get(fsConfig)
    val outputPath = new Path("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/Ulysses_Scala");
    if (fs.exists(outputPath)){
      fs.delete(outputPath, true)
    }
    val textFile = spark.textFile("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Books/Ulysses.txt");
    val words = textFile.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey{case(x, y) => x + y}
    counts.saveAsTextFile("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/Ulysses_Scala")
  }
}