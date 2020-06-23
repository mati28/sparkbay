import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
object WeatherStationTwo {
	def parseLine(line:String) = {
		val lines = line.split(",")
		val station= lines(0)
		val maxTemp = lines(2)
		val temp= lines(3).toFloat* 0.1f * (9.0f / 5.0f) + 32.0f
		(station,maxTemp,temp)
	}
	def main(args: Array[String]){

		val sc = new SparkContext("local","WeatherTwo")

		val line = sc.textFile("/home/linuxguy/ai/bigdata/SparkScala/1800.csv")
		val stationTemperatures = line.map(parseLine)
		val maxTemp = stationTemperatures.filter(entry=> entry._2=="TMAX")
		val temperature = maxTemp.map(element=> (element._1, element._3.toFloat))

		val tempReduced = temperature.reduceByKey((x,y)=> if (x>y) x else y).collect()

		for (temp <- tempReduced.sorted)	{

			val station = temp._1
			val temperature= temp._2
			val formatted = f"$temperature%.2f F"
			println(f"$station maximum temperature $formatted")
		}



	}
}