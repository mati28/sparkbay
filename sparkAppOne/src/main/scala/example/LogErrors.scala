package example
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object LogErrors {

	def main(args: Array[String]) {
		val inputpath = args(0)
		val outputpath = args(1)

		val sc = new SparkContext()

		val rdd = sc.textFile(inputpath)

 val logErrors = rdd.filter( line => line.contains("ERROR") || line.contains("INFO"))
				.map(line => (line.split("-")(4),1))
				.reduceByKey(_ + _)
				.saveAsTextFile(outputpath)		
	}
}