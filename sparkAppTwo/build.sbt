name := "Spark Etl "
 
version := "1.0.0"
 
scalaVersion := "2.11.12"
 
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.20"
