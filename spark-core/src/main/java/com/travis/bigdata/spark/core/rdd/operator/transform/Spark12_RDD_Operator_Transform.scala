package com.travis.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf  = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //    todo   算子 -双value类型
    val rdd1: RDD[Int]  =  sc.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int]  =  sc.makeRDD(List(4, 3, 3, 4))
    val rdd7: RDD[String]  =  sc.makeRDD(List(",","ewr",""," "))

    //交集
    val rdd3 = rdd1.intersection(rdd2)
    println(rdd3.collect().mkString(","))
    //并集
    val rdd4 = rdd1.union(rdd2)
    println(rdd4.collect().mkString(","))
//    差集
    val rdd5 = rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))

//  拉链
    val rdd6 = rdd1.zip(rdd7)
    println(rdd6.collect().mkString(","))

    //    todo  关闭资源
    sc.stop()

  }
}
