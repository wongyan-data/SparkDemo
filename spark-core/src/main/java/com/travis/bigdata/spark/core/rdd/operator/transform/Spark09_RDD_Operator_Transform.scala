package com.travis.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //    todo  准备环境
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // todo   算子 -distinct

//    Scala集合使用的是HashSet
//    RDD使用的

    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4, 5,5,7,7,9,10)
    )

    val dRDD: RDD[Int] = rdd.distinct()


    dRDD.foreach(println)



  //    todo  关闭资源
    sc.stop()

  }

}
