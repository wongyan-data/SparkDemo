package com.travis.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Action {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc: SparkContext = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4)
    )

//    TODO - 行动算子

//  reduce
//    val i = rdd.reduce(_ + _)
//    println(i)


    //    collect:方法会将不同分区的数据根据分区采集到Driver端内存中，形成数组
    val ints: Array[Int] = rdd.collect()
    ints.foreach(println)

    val first: Int = rdd.first()
    println(first)


    val take: Array[Int] = rdd.take(3)
    print(take.mkString(","))





    sc.stop()

  }
}
