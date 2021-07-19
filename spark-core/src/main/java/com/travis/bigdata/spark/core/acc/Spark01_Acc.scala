package com.travis.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))


    var  sum = 0
    rdd.foreach(
      num => {
        sum += num
      }
    )

    println("sum = " + sum)

    sc.stop()
  }

}
