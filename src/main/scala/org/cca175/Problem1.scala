package org.cca175
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import com.databricks.spark.avro._
object Problem1 {
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("FilterSparkRDD - Scala")
    val spark = new SparkContext(conf)
    val sqlContext = new SQLContext(spark)
    var ordersDF = sqlContext.read.avro("hdfs://hadoop.master.com:9000/user/hadoop/cca175/problem1/orders"); 
    var orderItemsDF = sqlContext.read.avro("hdfs://hadoop.master.com:9000/user/hadoop/cca175/problem1/order_items");
    println("orderDF Count -> " + ordersDF.count());
    println("orderItemsDF Count -> " + orderItemsDF.count());
    println("orderDF first -> " + ordersDF.first());
    println("orderItemsDF first -> " + orderItemsDF.first());
    ordersDF.printSchema();
    orderItemsDF.printSchema()
    var joinedOrderDF = ordersDF.join(orderItemsDF, ordersDF("order_id") === orderItemsDF("order_item_order_id"));
    println("joinedOrderDF Count -> " + joinedOrderDF.count());
    println("joinedOrderDF first -> " + joinedOrderDF.first());
    joinedOrderDF.printSchema();
  }
}