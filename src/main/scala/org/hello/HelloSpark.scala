package org.hello

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object HelloSpark {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("HelloSparkScala").setMaster("spark://hadoop.master.com:7077")
    val sc = new SparkContext(conf)
    val textfile = sc.textFile("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Books/The_Outline_of_Science.txt")
    println("No of Lines -> " + textfile.count())
    println("Completed....");
  }
}