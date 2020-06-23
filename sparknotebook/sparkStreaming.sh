import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext

val ssc = new StreamingContext(sc, Seconds(10))

val lines  = ssc.socketTextStream("localhost",9999)

val wordCount = lines.flatMap(line => line.split(" ")).
    map(word=> (word,1)).
    reduceByKey((a,b)=> a+b)

def myPrint(stream:DStream[(String, Int)]) {
    stream.foreachRDD(func)
    def func = (rdd:RDD[(String,Int)])=>{
        val arr = rdd.collect()
        for(elem<-arr){
            println(elem)
        }
    }
}
myPrint(wordCount)
ssc.start()

ssc.awaitTermination()

//val window  = wordCount.countByWindow(Seconds(12),Seconds(4))