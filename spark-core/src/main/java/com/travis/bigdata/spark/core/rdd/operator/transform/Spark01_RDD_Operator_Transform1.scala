package com.travis.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    //    todo  准备环境
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // todo   算子 -map
    //    178.191.44.227 - - 17/05/2015:12:05:05 +0000 GET /projects/xdotool/xdotool.xhtml
    val rdd: RDD[String] = sc.textFile("data/apache.log")
    val mapRDD: RDD[String] = rdd.map(
      line => {
        val data: Array[String] = line.split(" ")
        data(6)
      }

    )

    mapRDD.collect().foreach(println)
  //    todo  关闭资源
    sc.stop()

  }

}
