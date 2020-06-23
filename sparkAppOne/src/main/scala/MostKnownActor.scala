import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object MostKnownActor {

  def loadData(): Map[Int, String] = {

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var dictNames: Map[Int, String] = Map()
    val lines = Source.fromFile("/home/linuxguy/ai/bigdata/SparkScala/ml-100k/u.item").getLines

    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        dictNames += (fields(0).trim().toInt -> fields(1))
      }
    }

    dictNames
  }

  def actorNames(line: String) = {
    val lines = line.split('\"')
    (lines(0).trim().toInt, lines(1))
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext()

    var dictIdAndName = sc.broadcast(loadData)

    val nameRdd = sc.textFile("/home/linuxguy/ai/bigdata/SparkScala/marvel-names.txt")
    //val movieRdd = sc.textFile("/home/linuxguy/ai/bigdata/SparkScala/ml-100k/u.item")

    val actorIdAndMovieTitle = nameRdd.map(actorNames)
    //val actorFollowerCount = actorFollower.reduceByKey((x, y) => x + y)

    val actorNameAndMovie = actorIdAndMovieTitle.map(x => (dictIdAndName.value(x._1), x._2)).collect
    actorNameAndMovie.foreach(println)
  }
}
