
package net.atos.spark

import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{desc, sum, udf}

object SparkEtl {
	
	case class Payment(physician_id:String,
		date_payment:String,record_id:String,payer:String,
		amount:Double,physician_specialty:String,
		nature_of_payment:String)

	def normalize(field:String):Double = {

		try{
			field.toDouble
		} catch {
			case n: NumberFormatException => {
				print("Cannot cast "+ field)
				-1
			}
		}		
	}

	def main (args: Array[String]):Unit = {

		val spark =  SparkSession.
		builder().
		appName("").
		getOrCreate()

		//   I. Read data from csv and register in temp table .

		val input = args(0)

		val rawDF = spark.read.option("header","true").csv(input)
		// we define a user define function 
		//that takes a string and return Double and we specify its arguments will be 
		//provided later
		import spark.implicits._

		val normalizeUDF = udf[Double,String](normalize(_))

		val df = rawDF.withColumn("amount",normalizeUDF($"Total_Amount_of_Payment_USDollars"))

		df.createOrReplaceTempView("payments")

		// II. Transform into a Dataset of payment Object

		val ds: Dataset[Payment] = spark.sql("select Physician_Profile_ID as physician_id, "+
		"Date_of_Payment as date_payment, "+
		"Record_ID as record_id, "+
		"Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name as payer, "+
		"amount, "+
		"Physician_Specialty as physician_specialty, "+
		"Nature_of_Payment_or_Transfer_of_Value as nature_of_payment "+
		"from payments "+
		"where Physician_Profile_ID is not null".stripMargin).as[Payment]
		ds.cache()

		// III explore Dataset


		// let's review the 20 first record
		ds.show(20);
		// print the schema
		ds.printSchema()

		// What are the Nature of Payments with reimbursement amounts greater than $1000 ordered by count?
		  ds.filter($"amount" > 1000).groupBy("nature_of_payment")
		    .count().orderBy(desc("count")).show(5)

		  // what are the top 5 nature of payments by count?
		  ds.groupBy("nature_of_payment").count().
		    orderBy(desc("count")).show(5)

		  // what are the top 10 nature of payment by total amount ?
		  ds.groupBy("nature_of_payment").agg(sum("amount").alias("total"))
		    .orderBy(desc("total")).show(10)

		  // What are the top 5 physician specialties by total amount ?
		  ds.groupBy("physician_specialty").agg(sum("amount").alias("total"))
		    .orderBy(desc("total")).show(5,false)

	    /*
      IV - Saving JSON Document
   

	  // Transform the dataset to an RDD of JSON documents :
	  ds.write.mode("overwrite").json("/home/achraf/Desktop/labo/SparkLab/datasets/open_payments_csv/OP_DTL_GNRL_PGYR2018_P06282019_demo.json")

	  /*
	      V - Saving data to database
	   */

	  val url = "jdbc:postgresql://localhost:5432/postgres"
	  import java.util.Properties
	  val connectionProperties = new Properties()
	  connectionProperties.setProperty("Driver", "org.postgresql.Driver")
	  connectionProperties.setProperty("user", "spark")
	  connectionProperties.setProperty("password","spark")

	  val connection = DriverManager.getConnection(url, connectionProperties)
	  println(connection.isClosed)
	  connection.close()

	  val dbDataFrame = spark.read.jdbc(url,"payment",connectionProperties)
	  dbDataFrame.show()

	  ds.write.mode("append").jdbc(url,"payment",connectionProperties)
	  dbDataFrame.show()*/

	}
}



