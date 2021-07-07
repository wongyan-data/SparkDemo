package com.travis.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //    todo  准备环境
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // todo   算子 -coaleasce  -repatition（底层自动使用shuffer ，用来扩大分区）
//    数据不会被打乱重新组合，只是数据组合
//    可能会出现数据倾斜
//    todo 第二个参数shuffle  可以处理数据倾斜，将数据重新清洗分区


//    Scala集合使用的是HashSet
//    RDD使用的

    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4, 5,5,7,7,9,10),3
    )

    val dRDD: RDD[Int] = rdd.coalesce(2,true)

    dRDD.saveAsTextFile("output")
    dRDD.foreach(println)



  //    todo  关闭资源
    sc.stop()

  }

}
