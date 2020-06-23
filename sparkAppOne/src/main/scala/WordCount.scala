
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object WordCount {

	def main(args: Array[String]): Unit = {

		val inputpath= args(0)
		val outputpath=args(1)
		val sc = new SparkContext()
		val rdd = sc.textFile(inputpath)
		val rddSplit = rdd.flatMap(line=> line.split(","))
		.map(element=> element.trim())
		.map(word => (word,1))
		.reduceByKey(_ + _)
		.saveAsTextFile(outputpath)
	}
}