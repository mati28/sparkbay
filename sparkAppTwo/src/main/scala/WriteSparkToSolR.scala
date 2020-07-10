package net.atos.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.log4j._
import org.apache.spark.sql.functions._

object WriteSparkToSolR {

  case class Order(orderId: Int,
                   orderDate: String,
                   orderCustomerId: Int,
                   orderStatus: String)

  case class OrderItem(orderItemId: Int,
                       orderItemOrderId: Int,
                       orderItemProductId: Int,
                       orderItemQuantity: Int,
                       orderItemSubtotal: Float,
                       orderItemProductPrice: Float)

  def main(args: Array[String]): Unit = {

    // Log the log level to print only errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Create SparkSession object
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("Order revenue").
      getOrCreate()

      val orderInput = args(0)
      val orderItemInput = args(1)

    import spark.implicits._
    val orderRaw = spark.sparkContext.textFile("/data/bigdata/data/retail_db/orders")
    
    val orderDF = orderRaw map (order => {
      val orderList = order.split(",")
      Order(orderList(0).toInt, orderList(1), orderList(2).toInt, orderList(3))
    })
    // Create a Dataset of orders and register it
    val orderDS = orderDF.toDS()
    //orderDS.show(10)

    orderDS.createOrReplaceTempView("orders")

    // Read raw orderItems

    val orderItemRaw = spark.sparkContext.textFile("/data/bigdata/data/retail_db/order_items")
    val orderItemDF = orderItemRaw map (orderItem => {
      val orderItemList = orderItem.split(",")
      OrderItem(orderItemList(0).toInt, orderItemList(1).toInt,
        orderItemList(2).toInt, orderItemList(3).toInt,
        orderItemList(4).toFloat, orderItemList(5).toFloat)
    })
    // Create a Dataset of orderItems
   val orderItemDS = orderItemDF.toDS()

    //orderItemDS.show
    val zkhost="localhost:9983"
   orderItemDS.createOrReplaceTempView("orderItems")
    val revenue = spark.sql("select o.orderDate,sum(oi.orderItemSubtotal) as total, " +
      "max(oi.orderItemSubtotal) as maximum,min(oi.orderItemSubtotal) as minimum " +
      "from orders o join orderItems oi on o.orderId = oi.orderItemOrderId " +
      "group by o.orderDate " +
      "order by total desc")

var writeToSolrOpts = Map("zkhost" -> zkhost, "collection" -> "orderRevenue")
revenue.write.format("solr").options(writeToSolrOpts).save

  