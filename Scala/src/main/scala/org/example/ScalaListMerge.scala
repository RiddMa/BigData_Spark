package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class ScalaListMerge {

}

object ScalaListMerge {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("KVMerge").setMaster("yarn")
    val sc = new SparkContext(sparkConf)
    val text1 = sc.textFile("word1.txt")
    val text2 = sc.textFile("word2.txt")

    var textU = text1.union(text2).distinct()

    textU.saveAsTextFile("hdfs://master:9000/Spark4_new")
    sc.stop()
  }
}
