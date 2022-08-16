package com.data.scalatest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import util._

object SparkTest {
  def main(args: Array[String]): Unit= {
    println("Hola!")
    val spark= SparkSession.builder().appName("SparkTest").getOrCreate()
    val iris= spark.read.csv("hdfs://localhost:9000/user/data/iris_dataset/iris").toDF("sepal_length","sepal_width","petal_length","petal_width","species")
    iris.show(3)
    iris.count()
    iris.filter("sepal_length > 5").show()
  }
}
