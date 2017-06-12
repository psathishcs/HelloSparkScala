package org.hello

import org.apache.spark.sql.SparkSession

object HelloSparkSQL {
  def main(args: Array[String]){
    val spark = SparkSession
    .builder()
    .appName("HelloSparkSQL - Scala")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    
    val df = spark.read.json("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Json/Companie.json")
    df.printSchema()
    df.show()
    df.select("name").show()
    df.select("name", "founded_year" +1).show()
    
  }
}