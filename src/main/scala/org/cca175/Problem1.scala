package org.cca175
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import com.databricks.spark.avro._
object Problem1 {
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("FilterSparkRDD - Scala")
    val spark = new SparkContext(conf)
    val sqlContect = new SQLContext(spark)
    var ordersDF = sqlContect.read.avro("hdfs://hadoop.master.com:9000/user/hadoop/cca175/problem1/orders"); 
    var orderItemsDF = sqlContect.read.avro("hdfs://hadoop.master.com:9000/user/hadoop/cca175/problem1/order_items");
    println("orderDF Count -> " + ordersDF.count());
    println("orderItemsDF Count -> " + orderItemsDF.count());
    println("orderDF first -> " + ordersDF.first());
    println("orderItemsDF first -> " + orderItemsDF.first());
  }
}