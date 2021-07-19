package com.travis.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //   获取系统的累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    rdd.foreach(
      num => {
        sumAcc.add(num)
      }
    )

    println("sum = " + sumAcc)

    sc.stop()
  }

}
