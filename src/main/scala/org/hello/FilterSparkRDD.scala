package org.hello

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object FilterSparkRDD {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("FilterSparkRDD - Scala")
    val spark = new SparkContext(conf)
    val fsConfig = new Configuration()
    fsConfig.set("fs.defaultFS", "hdfs://hadoop.master.com:9000");
    val fs = FileSystem.get(fsConfig)
        
    val nums = spark.parallelize(List(1,2,3,5,6,7,8,20,235,366,3))
    val result = nums.map(x=> x*x)
    println(result.collect().mkString(" ,"))
    
    val text_file = spark.textFile("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Books/Ulysses.txt")
    println("No of Lines -> " + text_file.count())
    println("First -> " +   text_file.first());
    val newsLines  = text_file.filter { line => line.contains("news")} 
    val outputPath = new Path("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/Ulysses_FilterNews_Scala");
    if (fs.exists(outputPath)){
      fs.delete(outputPath, true)
    }
    newsLines.saveAsTextFile("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/Ulysses_FilterNews_Scala")
   }
}