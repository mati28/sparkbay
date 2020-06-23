import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
object MostRatted {
  def main(args:Array[String]): Unit = {


    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext()
    //val inputPath = args(0)
    //val outputPath = args(1)

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("/home/linuxguy/ai/bigdata/SparkScala/ml-100k/u.data")

    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(x => x.toString().split("\t")(2))

    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue()

    // Sort the resulting map of (rating, count) tuples
    val sortedResults = results.toSeq.sortBy(_._1)

    // Print each result on its own line.
    sortedResults.foreach(println)

  }
}
