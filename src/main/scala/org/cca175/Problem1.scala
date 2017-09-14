package org.cca175
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import com.databricks.spark.avro._
import org.apache.spark.sql.functions._;
/**
 * root
 |-- order_id: integer (nullable = true)
 |-- order_date: long (nullable = true)
 |-- order_customer_id: integer (nullable = true)
 |-- order_status: string (nullable = true)
 |-- order_item_id: integer (nullable = true)
 |-- order_item_order_id: integer (nullable = true)
 |-- order_item_product_id: integer (nullable = true)
 |-- order_item_quantity: integer (nullable = true)
 |-- order_item_subtotal: float (nullable = true)
 |-- order_item_product_price: float (nullable = true)
 * 
 */
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
    joinedOrderDF.show();
    joinedOrderDF.select("order_status").show();
    println("Filter........................>");
    joinedOrderDF.filter(joinedOrderDF("order_customer_id") === 2755).show();
    joinedOrderDF.groupBy("order_status").count().show();
    //formated Date
    joinedOrderDF.select(to_date(from_unixtime((col("order_date")/1000)))).alias("Order_Formatted_Date").show();
    joinedOrderDF.groupBy(to_date(from_unixtime((col("order_date")/1000)))).count().show();
    
    //Multiple Groupe by 
    joinedOrderDF.groupBy(to_date(from_unixtime((col("order_date")/1000))), col("order_status")).count().show();
    joinedOrderDF
          .groupBy(to_date(from_unixtime((col("order_date")/1000))).alias("order_formatted_date"), col("order_status"))
          .agg(round(sum("order_item_subtotal"),2).alias("total_amount"),countDistinct("order_id").alias("total_order"))
          .orderBy(col("order_formatted_date").desc,col("order_status"), col("total_amount").desc,col("total_order")).show();
    joinedOrderDF.createOrReplaceTempView("order_joined");
    
    var sqlResult = sqlContext.sql("select to_date(from_unixtime(cast(order_date/1000 as bigint))) as order_formatted_date, order_status, cast(sum(order_item_subtotal) as DECIMAL (10,2)) as total_amount, count(distinct(order_id)) as total_orders from order_joined group by to_date(from_unixtime(cast(order_date/1000 as bigint))), order_status order by order_formatted_date desc,order_status,total_amount desc, total_orders");
    sqlResult.show();
    sqlContext.sql("SELECT to_data(from_unixtime(case(order_date)/1000 as bigint))) as order_formatted_data, order_status, case(sum(order_item_subtotal)as DECIMAL(10,2) from order_joined");
  }
}