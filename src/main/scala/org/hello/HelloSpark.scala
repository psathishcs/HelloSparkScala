package org.hello

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object HelloSpark {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("HelloSparkScala")
    val sc = new SparkContext(conf)
    
    val nums = sc.parallelize(List(1,2,3,5,6,7,8,20,235,366,3))
    val result = nums.map(x=> x*x)
    println(result.collect().mkString(" ,"))
    
    val textfile = sc.textFile("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Books/The_Outline_of_Science.txt")
    println("No of Lines -> " + textfile.count())
    println("Completed....");
  }
}