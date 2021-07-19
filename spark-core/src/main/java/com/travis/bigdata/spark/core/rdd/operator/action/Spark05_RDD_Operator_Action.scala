package com.travis.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Action {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc: SparkContext = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4)
    )

    //    TODO - 行动算子
//  其实是Driver端内存集合的循环遍历方法
   rdd.collect().foreach(println)
    println("=========================")
//    其实是Excutor端内存的数据打印
    rdd.foreach(println)

    sc.stop()

  }
}
