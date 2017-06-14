package org.hello

import org.apache.spark.sql.SparkSession

case class Employees( 
  emp_no: Long,
  birth_date: String,
  first_name: String,
  last_name: String,
  gender: String,
  hire_date: String
)

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
   // employeesDF.show()
  
    
  }
}
