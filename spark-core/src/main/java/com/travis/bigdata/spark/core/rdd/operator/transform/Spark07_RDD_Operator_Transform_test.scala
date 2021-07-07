package com.travis.bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Transform_test {
  def main(args: Array[String]): Unit = {
    //    todo  准备环境
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // todo   算子 -filter

    val rdd: RDD[String] = sc.textFile("data/apache.log")
    rdd.filter(
       line => {
         val data: Array[String] = line.split(" ")
         val time = data(3)
         time.startsWith("17/05/2015")
       }
    ).collect().foreach(println)






  //    todo  关闭资源
    sc.stop()

  }

}
