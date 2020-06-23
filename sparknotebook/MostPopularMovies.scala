//The most popular Super Heros

// Method I

/*
try {
        Some(Integer.parseInt(in.trim))
    } catch {
        case e: Exception => None
    }
 */ 
 // val bag = List("1", "2", "three", "4", "one hundred seventy five") 

def friendParser(line:String) = {
	val list = line.split("\\s+")
	(list(0).toInt,list.length-1)
}

def nameParser(line:String): Option[(Int,String)]={
	val list = line.split('\"')
	if (list.length > 1){
		Some(list(0).trim.toInt,list(1))
	} else {
		None
	}   
}
// Read super hero and their connections
val data = sc.textFile("/home/linuxguy/Downloads/SparkScala/marvel-graph.txt")

// Apply the func to the dataset
val friendConnections = data.map(friendParser)

val friendCountConnection = friendConnections.reduceByKey((a,b) => a+b)


// Load the names data
val names = sc.textFile("/home/linuxguy/Downloads/SparkScala/marvel-names.txt")
//Apply the func the data
val namePair = names.map(nameParser).
flatMap(line=> line)

//Join friendConnections and namePair  (friendId,(connections,name))

val nameFriendPair =  friendCountConnection.join(namePair).
map({case (id,(con,name)) => (con,name)}).
sortByKey(false).
take(1)

// Method II

def friendParser(line:String) = {
	val list = line.split("\\s+")
	(list(0).toInt,list.length-1)
}

def nameParser(line:String): Option[(Int,String)]={
	val list = line.split('\"')
	if (list.length > 1){
		Some(list(0).trim.toInt,list(1))
	} else {
		None
	}   
}

// Read super hero and their connections
val data = sc.textFile("/home/linuxguy/Downloads/SparkScala/marvel-graph.txt")

// Apply the func to the dataset
val friendConnections = data.map(friendParser)

val friendCountConnection = friendConnections.reduceByKey((a,b) => a+b)

val maxConnected = friendCountConnection.map(friend=> friend.swap).max

// Load the names data
val nameData = sc.textFile("/home/linuxguy/Downloads/SparkScala/marvel-names.txt")
//Apply the func the data
val namePair = names.flatMap(nameParser)

val heroName = namePair.lookup(maxConnected._2)(0)

val  heroFriends = maxConnected._1

val line = s"$heroName is the most popular superHero with $heroFriends friends" 

#####################################################################

//Method III

package net.atos.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec


object MostPopular {

	def maxConnected() = {

		//implicit val codec = Codec("UTF-8")
    	//codec.onMalformedInput(CodingErrorAction.REPLACE)
    	//codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

		val lines = Source.fromFile("/home/linuxguy/Downloads/SparkScala/marvel-graph.txt").getLines
		
		val friendMap :collection.mutable.Map[Int,Int] = collection.mutable.Map()

		for (line <- lines) {

			val list = line.split("\\s+")
			val key = list(0).trim.toInt
			val value = list.length-1

			val elem = friendMap.get(key)

			if (elem == None) {
			    friendMap += (key -> value)
			} else {
			    friendMap.update(key, elem.get + (list.length-1))
			}
	    }

    	friendMap.map(_.swap).max 
	}

	def nameParser(line:String): Option[(Int,String)]={
		val list = line.split('\"')
		if (list.length > 1){
			Some(list(0).trim.toInt,list(1))
		} else {
			None
		}   
	}

	def main(args:Array[String]) {


		// Set the log level to only print errors
    	Logger.getLogger("org").setLevel(Level.ERROR)
    
     	// Create a SparkContext using every core of the local machine
    	val sc = new SparkContext("local[*]", "PopularMoviesNicer")  
    
    	// Create a broadcast variable of our (connections, superheroId) tuple
    	var mostConnected = sc.broadcast(maxConnected)

    	val  data = sc.textFile("/home/linuxguy/Downloads/SparkScala/marvel-names.txt")

    	val friendsMapRDD = data.flatMap(nameParser) 

    	//val superHeroName = friendsMapRDD.lookup(mostConnected.value)(0)

    	println(s"$superHeroName is the most connected with $mostConnected friends")









	}





}



	




