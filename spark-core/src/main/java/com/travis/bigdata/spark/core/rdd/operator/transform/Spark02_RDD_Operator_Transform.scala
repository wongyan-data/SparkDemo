package com.travis.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //    todo  准备环境
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // todo   算子 -mapPartitions
    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2,3,4),2
    )

/*
  rdd.mapPartitions():可以将分区为单位进行操作
 */

    val mapRDD: RDD[Int] = rdd.mapPartitions(
      iter => {
        println(">>>>>>>>>>>>>")
        iter.map(_ * 2)
      }
    )



    mapRDD.collect().foreach(println)

  //    todo  关闭资源
    sc.stop()

  }

}
