import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object PopularMovie {
	def parseLine(lines: String)={
		val line = lines.split("\\s+")
		val movieId = line(1)
		(movieId, 1)
	}

	def main(args: Array[String]){

		val sc = new SparkContext("local","PopularMovie")

		val rdd = sc.textFile("/home/linuxguy/ai/bigdata/SparkScala/ml-100k/u.data")

		val movieMap = rdd.map(parseLine)

		val movieReduce = movieMap.reduceByKey((countFisrt, countSecond)=> countFisrt + countSecond)

		val movieValueKey = movieReduce.map(x=> (x._2,x._1))
		val sortedMovie = movieValueKey.sortByKey().collect()

		sortedMovie.foreach(println)


	}
}