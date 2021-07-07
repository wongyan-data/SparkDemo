package com.travis.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf  = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //    todo   算子 -GroupBykey
    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(("a", 2), ("a", 4), ("c", 2),
        ("b", 2), ("b", 4), ("b", 2)
      )
    )
    rdd.foldByKey(0)(_+_).collect().foreach(println)




    //    todo  关闭资源
    sc.stop()

  }
}
