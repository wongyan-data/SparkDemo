package com.travis.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {


    //    todo  准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

//    todo  创建RDD
//    从内存中创建RDD
    val sep  = Seq[Int](1,2,3,4)
    //    val rdd: RDD[Int] = sc.parallelize(sep)
    val rdd: RDD[Int] = sc.makeRDD(sep)
    rdd.collect().foreach(println)

    //    todo  关闭资源
    sc.stop()
  }

}
