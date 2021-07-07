package com.travis.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform_test {
  def main(args: Array[String]): Unit = {
    //    todo  准备环境
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // todo   算子 -glom
    val rdd: RDD[Int] = sc.makeRDD(

        List(1,2,3,4, 5),2

    )
    val glomRDD: RDD[Array[Int]] = rdd.glom()

    val maxRDD: RDD[Int] = glomRDD.map(
      array => {
        array.max
      }
    )
    println(maxRDD.collect().sum)





  //    todo  关闭资源
    sc.stop()

  }

}
