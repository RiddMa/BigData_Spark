package org.example

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties

class InsertStudent {

}

object InsertStudent {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("insert-student").setMaster("local")
    val sc = new SparkContext(sparkConf)
    //学生信息RDD
    val studentRDD = sc.parallelize(Array("3 Lu M 21", "4 Shi M 21")).map(_.split(" "))
    val schema = StructType(List(
      StructField("id", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("gender", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true))) //Row对象
    val rowRDD = studentRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt))
    val studentDF = new SQLContext(sc).createDataFrame(rowRDD, schema)
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "Mjj123456;")
    prop.put("driver", "com.mysql.jdbc.Driver") //连接数据库，append
    studentDF.write.mode(saveMode = "append").jdbc(url = "jdbc:mysql://localhost:3306/spark",
      table = "spark .student", prop)
    println(prop.toString)
  }
}