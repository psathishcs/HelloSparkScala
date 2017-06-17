package org.hello

import org.apache.spark.sql.SparkSession
import com.github.nscala_time.time.Imports._
import org.hello.entity.Employees

object EmployeesSparkSQL {
  def main(args: Array[String]) {
          val spark = SparkSession
    .builder()
    .appName("EmployeesSparkSQL - Scala")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    import spark.implicits._
    
    val employeesDF = spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv("hdfs://hadoop.master.com:9000/user/psathishcs/Input/csv/Employees.csv").as[Employees]
    employeesDF.printSchema()
  }
}
