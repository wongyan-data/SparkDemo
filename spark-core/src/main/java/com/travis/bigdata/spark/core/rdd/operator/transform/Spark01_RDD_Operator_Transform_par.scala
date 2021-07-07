package com.travis.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_par {
  def main(args: Array[String]): Unit = {
    //    todo  准备环境
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // todo   算子 -map
    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2,3,4),1
    )
    val mapRDD: RDD[Int] = rdd.map(
      num => {
        println("##########   " + num)
        num
      }
    )

    val mapRDD1: RDD[Int] = mapRDD.map(
      num => {
        println(">>>>>>>>>   " + num)
        num
      }
    )

    mapRDD1.collect()

  //    todo  关闭资源
    sc.stop()

  }

}
