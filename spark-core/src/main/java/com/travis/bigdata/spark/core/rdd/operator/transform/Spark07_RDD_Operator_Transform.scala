package com.travis.bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //    todo  准备环境
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // todo   算子 -filter

    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4)
    )

    val fRDD: RDD[Int] = rdd.filter( num => num % 2 != 0)

    fRDD.collect().foreach(println)



  //    todo  关闭资源
    sc.stop()

  }

}
