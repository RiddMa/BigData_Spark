package org.example

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

class KafkaStreaming {

}

object KafkaStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("kafka_test")
    val ssc = new StreamingContext(conf, Seconds(10))

    val zkQuorum = "localhost:2181"
    val group = "1"
    val topics = "wordsendertest"
    val numThreads = 1;
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lineMap = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)

    val lines = lineMap.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map((_,1)).reduceByKey(_+_)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
