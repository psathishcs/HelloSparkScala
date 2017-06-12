package org.hello

import org.apache.spark.sql.SparkSession

object HelloSparkSQL {
  def main(args: Array[String]){
        val spark = SparkSession
    .builder()
    .appName("HelloSparkSQL - Scala")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    import spark.implicits._
    
    val df = spark.read.json("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Json/Companie.json")
    df.printSchema()
    df.show()
    df.select("name").show()
    df.select($"name", $"founded_year" +1).show()
    
    val dfs = spark.read.json("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Json/Companies.json")
    dfs.filter($"founded_year" > 2010).show()
    println("Count of $founded_year > 2010 ------> " +  dfs.filter($"founded_year" > 2010).count())
    df.createOrReplaceTempView("Companie")
    dfs.createOrReplaceTempView("Companies")
    val foundedYearSQL = spark.sql("SELECT * FROM Companies WHERE  founded_year >= 2005")
    foundedYearSQL.show()
    println("Count of SELECT * FROM Companies WHERE  founded_year >= 2005 ------> " +  foundedYearSQL.count())
    
    val foundedYearListSQL = spark.sql("SELECT * FROM Companies WHERE  founded_year IN (SELECT founded_year FROM Companie WHERE founded_year != NULL)")
    foundedYearListSQL.show()
    println("Count of SELECT * FROM Companies WHERE  founded_year IN (SELECT founded_year FROM Companie) ------> " +  foundedYearListSQL.count())
     
  }
}