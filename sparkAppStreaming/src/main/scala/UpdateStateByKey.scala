package net.atos.spark

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

object UpdateStateByKeyWordCount{

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]):Option[Int] = {

	    val newCount = runningCount.getOrElse(0) + newValues.sum
		Some(newCount)
  }
  def main(args: Array[String]) {
      val spark = SparkSession
                   .builder
                   .appName(getClass.getSimpleName)
                   .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(40))
    ssc.checkpoint("/tmp")
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val runningCounts = pairs.updateStateByKey[Int](updateFunction _)
    runningCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}