package com.travis.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Persist {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(sparkConf)


    val list: List[String] = List("hello scala", "hello spark")
    val rdd: RDD[String] = sc.makeRDD(list)
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map((_, 1))
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)
println("========================")


    val groupRDD= mapRDD.groupByKey()

    groupRDD.collect().foreach(println)
  }
}