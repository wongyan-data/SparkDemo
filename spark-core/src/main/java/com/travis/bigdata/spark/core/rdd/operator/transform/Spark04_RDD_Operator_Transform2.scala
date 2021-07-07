package com.travis.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {
    //    todo  准备环境
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // todo   算子 -flatmap
    val rdd: RDD[Any] = sc.makeRDD(
      List(
        List(1, 2), 3, List(4, 5)
      )
    )
    val flatRDD: RDD[Any] = rdd.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case dat => List(dat)
        }
      }
    )





    flatRDD.collect().foreach(println)

  //    todo  关闭资源
    sc.stop()

  }

}
