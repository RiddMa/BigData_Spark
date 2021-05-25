package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class ScalaWordCount {

}

object ScalaWordCount {
  def main(args: Array[String]): Unit = {
    val list = List("Hello hi hi spark",
      "hello spark hello hi spark_sql",
      "hello hi hi spark_streaming",
      "hello hi spark_graphx")
    val sparkConf=new SparkConf().setAppName("WordCount").setMaster("yarn")
    val sc = new SparkContext(sparkConf)
    val lines:RDD[String]=sc.parallelize(list)
    val words:RDD[String]=lines.flatMap((line:String)=>{line.split(" ")})
    val wordKVOne:RDD[(String,Int)]=words.map((word:String)=>{(word,1)})
    val wordKVNum:RDD[(String,Int)]=wordKVOne.reduceByKey((count1:Int,count2:Int)=>{count1+count2})
    val ret = wordKVNum.sortBy(kv => kv._2,false)
    print(ret.collect().mkString(","))
    ret.saveAsTextFile("hdfs://master:9000/Spark")
    sc.stop()
  }
}
