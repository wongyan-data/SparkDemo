package com.travis.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //    todo  准备环境
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // todo   算子 -coaleasce  -repatition（底层自动使用shuffer ，用来扩大分区）



    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 30, 4, 5,5,7,7,9,10),3
    )

    val sortRDD: RDD[Int] = rdd.sortBy(num=>num,false)

    sortRDD.saveAsTextFile("output")
    sortRDD.foreach(println)



  //    todo  关闭资源
    sc.stop()

  }

}
