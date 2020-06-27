*/Spark solves MapReduce latency by keeping all data immutable and in memory*/

//Transformers and accessors in  scala

//Transformers are operations that return new collections as result


//Accessors (reduce, fold, aggregate) are methods that return a single value from collections

//Transformations  in spark return new RDDs as result; they are lazy
// map, flatMap, filter, distinct

// Actions compute a result based on RDD and either return or save to an external storage 
system (HDFS); they are eager

collect, take => Array[T],count=>Long, reduce, foreach=> Unit

Actions specifiques to spark not found in scala

takeSample,takeOrdered=>Array[T]
saveAsTextFile=>Unit
saveAsSequenceFile=>Array[T]


==================================================
// Pair_RDD Operations not available for simple RDD

Transformations:

groupByKey,reduceByKey,mapValues,keys,join, leftOuterJoin,RightOuterJoin

Actions:

reduce

===================================================================
shuffle and partition:
RDD is constituted of partitions and the number of 
partitions determine the parallelism of a job

*By default the number of partitions equal the total numbers of
cores on the cluster
* customizing partition is only possible for key pair RDD
SET PARTITION FOR data
 a) call partitionBy on RDD by providing a specific partitioner
 (rangePartition, hashPartition)
 b) using transformations that returns RDD with specifics partitioner

 pair RDD that hold to and propagate partitioner

 reduceByKey, groupByKey, foldByKey, sort, join, leftOuterJoin, rightOuterJoin,
  groupWith, cogroup, flatMapValues, mapValues, filter (if parent has partitioner)
 combineByKey, partitionBy

 all other transformations produce result without partitioner

=========================================================================
invoking partionBy creates an RDD with a specific partitioner
val  pairs = purchasesRdd.map(p => (p.customerId, p.price))
val tunedPartitioner = new RangePartitioner(8, pairs)
val partitioned = pairs.partitionBy(tunedPartitioner).persist()  // 
// the result of partitionBy should always be persisted
groupByKey => HashPartitioner
sortByKey => RangePartitioner
==========================================================================
Spark SQL is a componenemt of Spark Stack
Dataframe are conceptually, RDDs full of records with a known schema.
Unlike RDDs Dataframes require schema info
Datasets can be thought of as a typed distributed collections of data
Dataset API unifies Dataframe and RDD APIs. Mix Match
Datasets required structured and semi-structured data. Schema and Encoders
are core part of Dataset. 
#Dataframe transformations: select,groupBy,orderBy,where,filter,as,sort,limit,union,drop
#Dataframe actions:show, collect,count, take,first

join types: inner, outer, left_outer,right_outer

df1.join(df2,"df1.id" === "df2.id") 
groupBy returns a type called RelationalGroupedDataset which contains (count, max, min,sum avg...).
#It has three main APIs
#SQL literal syntax
#Dataframe //
#They are conceptually RDDs full of records with a known schema
#DataFrame creation
1. from RDDs
Either by infering a schema or explicitly defining one
2. from csv, json...... files
3. Refering to columns
eleDF.filter($"age" > 18)
eleDF.filter(df("colname") > 18)
eleDF.filter("colname > 18")
#Dataset
=======================================================================
hdfs dfs -ls
hdfs dfs -ls -R -h /var

hdfs dfs -mkdir fichier
=======================================================================
to compile in sbt
compile

=========================================

=======================================================================
read table from MySQL DB into spark DataFrame

1. spark-shell --jars mysql-connector-java-5.1.46.jar --driver-class-path mysql-connector-java-5.1.46.jar

2. val orders= spark.read.
	format("jdbc").
	option("url", "jdbc:mysql://localhost/mydb").
	option("dbtable", "orders").
	option("user","root").
	option("password","passer").
	load()
======================================================
read table from MySQL DB into spark DataFrame

val df_mysql = spark.read.format(“jdbc”)
   .option(“url”, “jdbc:mysql://localhost:port/db”)
   .option(“driver”, “com.mysql.jdbc.Driver”)
   .option(“dbtable”, “tablename”) 
   .option(“user”, “root”) 
   .option(“password”, “password”) 
   .load()

achatDF.write.
format("org.apache.spark.sql.jdbc").
mode("ignore").
options(Map(
"url" -> "jdbc:mysql://localhost/commerce?user=root&password=passer",
"dbtable" -> "achat2")).
save()


// Spark SQl thriftserver 

CREATE TEMPORARY TABLE biz USING org.apache.spark.sql.json 
OPTIONS (path "/home/linuxguy/Desktop/dataset/business.json");
//

// To find where hive is writing file in HDFS:

1. cd /etc/hive/conf/
2. vi hive-site.xml
3. Look for hive metastore section

// From hive cli

hive>(retail_db) set hive.metastore.warehouse.dir;

create table trial(
orderItemId int,
orderItemOrderId int,
orderItemProductId int,
orderItemQuantity int,
orderItemSubtotal float,
orderitemProductPrice float)
row format delimited fields terminated by ',';

// to run spark on top hive

1. launch spark-shell

use the sparkSession onbject created (spark or sqlContext)

spark.sqlContext.sql("query goes here")

hive>(retail_db) show functions

hive>(retail_db) describe function length // to see detail of specific function


// to use less threads while running a query use:

sqlContext.setConf("spark.sql.shuffle.partitions","2")

case class Payment(pysician_id:String,date_payment:String,record_id:string,payer:String,amount:Double,physician_specialty:String,nature_of_payment:String)

df.createOrReplaceTempView("payments")

spark.sql("select pysician_id,date_payment,record_id,payer,amount,physician_specialty,nature_of_payment")
val subset =  spark.sql("")  


// Reading csv by defining a schema.

val schema = new StructType().add("sample","long").add("cThick", "integer").
add("uCSize", "integer").add("uCShape","integer").add("mAdhes", "integer").
add("sECSize", "integer").add("bNuc","integer").add("bChrom", "integer").
add("nNuc", "integer").add("mitosis","integer").add("clas", "integer")

val df = spark.read.format("csv").
option("header",false).
schema(recordSchema).
load("/data/bigdata/data/breast-cancer-wisconsin.data")

def binarize(input: Int) = input match {
  case 2 => 0
  case 4 => 1  
}

spark.udf.register("normalizerUDF", (input:Int) => binarize(input))

}