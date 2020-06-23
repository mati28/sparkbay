import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object MostKnownActor {

	def loadData() = {

		implicit val codec = Codec("UTF-8")
    	codec.onMalformedInput(CodingErrorAction.REPLACE)
    	codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

		var dictNames:Map[Int, String] = Map()
		val lines = Source.fromFile("/home/linuxguy/ai/bigdata/SparkScala/ml-100k/u.item").getLines
		
		for(line <- lines){
			val lines = line.split('|')
			if (lines.length > 1){
			dictNames += (lines(0).trim().toInt->lines(1))
			}
		}
		
		dictNames
	}

	def countFollowers (line:String) = {
		val lines = line.split("\\s+")
		(lines(0).toInt,lines.length-1)
	}

	def main(args:Array[String]){

		 // Set the log level to only print errors
    	Logger.getLogger("org").setLevel(Level.ERROR)

		val sc = new SparkContext("local[*]","MostKnownActor")

		val nameDict = sc.broadcast(loadData)

		val nameRdd = sc.textFile("/home/linuxguy/ai/bigdata/SparkScala/marvel-names.txt")

		val  actorFollower       = nameRdd.map(countFollowers)
		val actorFollowerCount = actorFollower.reduceByKey((x,y)=>x+y)

		val actor = actorFollowerCount.map(x=> (nameDict.value(x._1),x._2)).collect.foreach(println)
	}
}
