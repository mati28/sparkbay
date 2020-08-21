/**

Inverted Index own version inspired from Dean Wampler git


*/

package net.atos.spark

import java.io.File

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

import org.apache.spark.sql.Row

import org.apache.log4j._


object InvertedIndex {

	//Set of words to be removed
	val unwantedWords = Set("a","the","thus","therefore","for","in")
	//Function to select word to maintain
	def wordToKeep(word:String):Boolean= !unwantedWords.contains(word) 
	//Define a schema
	val schema = StructType(List(
      StructField("word",StringType,true),
      StructField("totalcount",IntegerType,true),
      StructField("locations",ArrayType(StringType, true),true),
      StructField("counts",ArrayType(IntegerType,true),true))
    )
	// Create a function that return a Row of element to be used
	def processor(spark:SparkSession,path:String) = {

		val IIndex = spark.sparkContext.wholeTextFiles(path).

		flatMap {

			case (uri,contents) => 

				val words = contents.split("""\W+""").filter(word=> word != "").map(word=> word.toLowerCase).
									filter(word => wordToKeep(word))
				val fileName = uri.split(File.separator).last

				words.map(word=> ((word,fileName),1))

		}.

		reduceByKey((count1,count2)=> count1+count2).

		map {
			case ((word,fileName),count) => (word,(fileName,count)) 
		}.

		groupByKey().

		sortByKey(ascending = true).

		map {

			case (word,iter) =>

				val vect = iter.toVector.sortBy {case (fileName,count) => (-count,fileName)}
				val (locations,counts) = vect.unzip
				val totalCount = counts.reduceLeft((a,b)=>a+b)

				Row(word,totalCount,locations,counts)
		}
		//Return explicitly Row of tuples
		IIndex

	}


	def main(args:Array[String]):Unit = {

		// Log the log level to print only errors
    	Logger.getLogger("org").setLevel(Level.ERROR)
		// Create a SparkSession object
		val spark = SparkSession.
		builder().
		appName("Inverted Index").
		getOrCreate()

		import spark.implicits._

		val input = args(0)

		//val path ="/data/bigdata/resources/certifications/books/JustEnoughScalaForSpark-master/data/shakespeare"

		val rowData = processor(spark,input)


		val invertedDF = spark.createDataFrame(rowData,schema)

		invertedDF.createOrReplaceTempView("inverted")

		spark.sql("select word,totalcount,locations[0]as top_location,counts[0] as top_count from inverted "+
			"where totalcount >= 10").show
	}

}

