import scala.io.Source

object OrderRevenue {

    def main(args: Array[String]) = {
        val orderId = args(1).toInt
        val orderItems = Source.fromFile("/data/bigdata/retail_db/order_items/part-00000").getLines
        val orderRevenue= orderItems.
                filter(article=> article.split(",")(1).toInt == orderId).
                map(order => order.split(",")(4).toFloat).
                reduce((v1,v2)=> v1 + v2)
                println(orderRevenue)


    }

}