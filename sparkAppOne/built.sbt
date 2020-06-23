name := "sparkscala"
 
version := "1.0.0"
 
scalaVersion := "2.11.12"
 
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.8"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"
