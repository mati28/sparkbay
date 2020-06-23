import java.io.File
val  wordCountPerFiles = sc.wholeTextFiles("/home/linuxguy/wordCountDir").
				flatMap { case (fileName2, content) =>

				val fileName = fileName2.split(File.separator).last
				val words = content.split("""\W+""").filter(word => word.length > 3)
				words.map(word => ((word,fileName),1)) }.
				reduceByKey { (v1,v2)=> v1 +v2 }.
				map { case ((word,fileName),count) => (word,(fileName, count)) }.
				groupByKey.sortByKey(ascending=true).
				mapValues { iterable => 
								val vect = iterable.toVector.sortBy { case (fileName,count) => (-count, fileName)

								}
							vect.mkString(",") 
						   }.
				collect.foreach(println)
#######################################################################

val  wordCountPerFiles = sc.wholeTextFiles("/home/linuxguy/wordCountDir").
				flatMap { case (fileName2, content) =>

				val fileName = fileName2.split(File.separator).last
				val words = content.split("""\W+""").filter(word => word.length > 3)
				words.map(word => ((word,fileName),1)) }.
				reduceByKey { (v1,v2)=> v1 +v2 }.
				map { case ((word,fileName),count) => (word.toLowerCase, count) }.
				reduceByKey((v1,v2) => v1 + v2).map(tuple => (tuple._2,tuple._1)).sortByKey(false).
			
				/*groupByKey.sortByKey(ascending=true).
				//reduceByKey(v1 ,v2) => 
				//mapValues { iterable => 
								val vect = iterable.toVector.sortBy { case (fileName,count) => (-count, fileName)

								}
							vect.mkString(",") 
						   }.*/
				take(1).foreach(println)
===========================================================================
drop()
drop("all")
drop(Array("clo1","clo2")).

===========================================================================
prediction/green

P000603362Q
P051616250H
PEARL DIARY FARMS LIMITED
Gabon
4407 
LLA
658540.62
4
==========================================================
prediction/yellow

P051517363N
P051155101I
ABS AUTU LINK (U) LIMITED
Brazil
8701
BSA
611288.612
12
================================================================================
P051176188L
P051664533G
ZA
MAKRO SA/A DIVISION MASSTORES (PTY) LIMI,
2204,
182257.731,
900,
True
================================================================================
spark video 68

