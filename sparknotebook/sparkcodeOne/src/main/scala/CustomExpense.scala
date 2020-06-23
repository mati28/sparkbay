import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object CustomExpense {
	def parseLine(line:String)={
		val lines = line.split(",")
		val customerId = lines(0).toInt
		val amount = lines(2).toFloat

		(customerId, amount)
	}

	def main(args:Array[String]){

		val sc = new SparkContext("local[*]","CustomExpense")

		val rdd = sc.textFile("/home/linuxguy/ai/bigdata/SparkScala/customer-orders.csv")

		val expense = rdd.map(parseLine).reduceByKey((x,y)=> x+y).map(x=> (x._2,x._1)).sortByKey().collect()

		expense.foreach(println)
	}


	
}