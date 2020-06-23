
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object WeatherStation {

  def parseLine(line:String) = {
    val lines = line.split(",")
    val station = lines(0)
    val min = lines(2)
    val temp = lines(3).toFloat
    (station,min,temp)
  }

  def main(args: Array[String]){


    val sc = new SparkContext()
    val lines = sc.textFile("/home/linuxguy/ai/bigdata/SparkScala/1800.csv")
    val rdd = lines.map(parseLine).filter(x=> x._2=="TMIN").map(x=> (x._1, x._3.toFloat))
    val reducevalue = rdd.reduceByKey((x,y)=> if(x<y) x else y)
    val resultat = reducevalue.collect()

    resultat.foreach(println)
  }

}
