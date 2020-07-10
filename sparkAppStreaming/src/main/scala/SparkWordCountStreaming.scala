package net.atos.spark
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

object SparkWordCountStreaming{

  def main(args: Array[String])
  {
      // Create Spark Session and Spark Context
     val spark=SparkSession.builder.
     appName(getClass.getSimpleName).
	 getOrCreate()
      // Get the Spark context from the Spark session to create streamingcontext

      val sc = spark.sparkContext
      // Create the streaming context, interval is 40 seconds
      val ssc = new StreamingContext(sc, Seconds(40))
      // Set the check point directory to save the data to recover when
	  //there is a crash
      ssc.checkpoint("/tmp")
	/* Create a DStream that connects to hostname:port to stream data from a
		TCP source.
	    S et the StorageLevel as StorageLevel.MEMORY_AND_DISK_SER which
		indicates that the data will be stored in memory and if it
		overflows, in disk as well*/

    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    // count the number of words in text data received from a data server
    //listening on a TCP socket.
    // Split each line into words
      val words = lines.flatMap(_.split(" "))
    // Count each word in each batch
       val pairs = words.map(word => (word, 1))
       val wordCounts = pairs.reduceByKey(_ + _)
    // Print the elements of each RDD generated in this DStream to the
    //console
        wordCounts.print()
    // Start streaming
        ssc.start()
    // Wait until the application is terminated
         ssc.awaitTermination()
  }
}