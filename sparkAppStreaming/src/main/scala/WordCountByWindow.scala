
package net.atos.spark
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
object WordCountByWindow{
  def main(args: Array[String])
  {
    // Create the Spark Session and the Spark Context
             val spark = SparkSession
                   .builder
                   .appName(getClass.getSimpleName)
                   .getOrCreate()
    // Get the Spark Context from the Spark Session to create streamingcontext
         
    val sc = spark.sparkContext


  	//Create the streaming context, interval is 10 seconds
     val ssc = new StreamingContext(sc, Seconds(10))
   	//Set the checkpoint directory to save the data to recover when there is a crash

    ssc.checkpoint("/tmp")
       // Create a DStream that connects to hostname:port to stream data
       //  from a TCP source.
       // Set the StorageLevel as StorageLevel.MEMORY_AND_DISK_SER which
       // indicates that the data will be stored in memory and if it
       // overflows, in disk as well
       // count the number of words in text data received from a data
       // server listening on a TCP socket.
       
    val lines = ssc.socketTextStream("localhost",9999, StorageLevel.MEMORY_AND_DISK_SER)
       // Split each line into words
        val words = lines.flatMap(_.split(" "))
       // Count each word in over the last 30 seconds of data
        val pairs = words.map(word => (word, 1))
           
	val wordCounts = pairs.reduceByKeyAndWindow((x: Int, y: Int) => x+y, Seconds(30), Seconds(10))

        wordCounts.print()
        // Start the streaming
              ssc.start()
        // Wait until the application is terminated
              ssc.awaitTermination()
  }
}