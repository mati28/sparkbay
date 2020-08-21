package net.atos.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.log4j._
import org.apache.spark.sql.functions._

object OrderRevenue {

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

    /*// Define a schema for order data
    val orderSchema = new StructType().
      add("orderId","integer").
      add("orderDate", "string").
      add("orderCustomerId", "integer").
      add("orderStatus", "string")
  // Define a schema for order_items data
    val orderItemSchema = new StructType().
      add("orderItemId","integer").
      add("orderItemOrderId", "integer").
      add("orderItemProductId", "integer").
      add("orderItemQuantity", "integer").
      add("orderItemSubtotal", "float").
      add("orderItemProductPrice", "float")
    // Create a DataFrame of orders
    val orderDF = spark.read.schema(orderSchema).option("header","false").
      csv("/data/bigdata/data/retail_db/orders")
    // Create a DataFrame of order_items
    val orderItemDF = spark.read.schema(orderItemSchema).option("header","false").
      csv("/data/bigdata/data/retail_db/order_items")

    //Print schema of the DFs created

    //orderItemDF.printSchema()

    //orderDF.printSchema()


    orderDF.join(orderItemDF, $"orderId" === $"orderItemOrderId").
      groupBy($"orderDate").agg(sum("orderItemSubtotal").
      alias("total"),max("orderItemSubtotal").
      alias("maximum"),min("orderItemSubtotal").
      alias("minimum")).sort(desc("total")).
      show(10)*/
    //Read raw orders
    import spark.implicits._
    val orderRaw = spark.sparkContext.textFile(orderInput)
    //
    val orderDF = orderRaw map (order => {
      val orderList = order.split(",")
      Order(orderList(0).toInt, orderList(1), orderList(2).toInt, orderList(3))
    })
    // Create a Dataset of orders and register it
    val orderDS = orderDF.toDS()
    //orderDS.show(10)

    orderDS.createOrReplaceTempView("orders")

    // Read raw orderItems

    val orderItemRaw = spark.sparkContext.textFile(orderItemInput)
    //
    val orderItemDF = orderItemRaw map (orderItem => {
      val orderItemList = orderItem.split(",")
      OrderItem(orderItemList(0).toInt, orderItemList(1).toInt,
        orderItemList(2).toInt, orderItemList(3).toInt,
        orderItemList(4).toFloat, orderItemList(5).toFloat)
    })
    // Create a Dataset of orderItems
   val orderItemDS = orderItemDF.toDS()

    //orderItemDS.show

    // aggregate accross orderDate (sum,max,min) of orderItemSubtotal

   orderItemDS.createOrReplaceTempView("orderItems")
    val revenue = spark.sql("select o.orderDate,sum(oi.orderItemSubtotal) as total, " +
      "max(oi.orderItemSubtotal) as maximum,min(oi.orderItemSubtotal) as minimum " +
      "from orders o join orderItems oi on o.orderId = oi.orderItemOrderId " +
      "group by o.orderDate " +
      "order by total desc")

    revenue.show(20)


    //============================


  }

}
