package com.travis.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf  = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //    todo   算子 -双value类型
    val rdd: RDD[Int]  =  sc.makeRDD(List(1, 2, 3, 4))
    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))
//    隐式转换（二次转换）
//    RDD => PairRDDFunction
//
    mapRDD.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")

   mapRDD.collect().foreach(println)




    //    todo  关闭资源
    sc.stop()

  }
}
