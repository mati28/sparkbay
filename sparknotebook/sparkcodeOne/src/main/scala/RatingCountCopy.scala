import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingCountCopy {
 
  /** Our main function where the action happens */
  def main(args: Array[String]):Unit = {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local","RatingsCounter")
    val inputPath = args(0)
    //val outputPath = args(1)
   
    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile(inputPath)
    
    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(x => x.toString().split("\t")(2))
    
    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue().toSeq.sortBy(_._1)
    
    results.foreach(println)
  }
}
