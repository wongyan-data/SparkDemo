package com.travis.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Persist {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(sparkConf)
sc.setCheckpointDir("cp")


    /**
     * cache、persist checkpoint  的区别
     */
    val list: List[String] = List("hello scala", "hello spark")
    val rdd: RDD[String] = sc.makeRDD(list)
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map(
      map => {
        println("@@@@@@@@@")
        (map, 1)
      }
    )
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
//    mapRDD.cache()
//   checkpoint 是需要落盘的，需要指定路径  一般都在hdfs中  ，现在就在本地进行模拟
    mapRDD.cache()
    mapRDD.checkpoint ()
    reduceRDD.collect().foreach(println)
 println("========================")


    val groupRDD= mapRDD.groupByKey()

    groupRDD.collect().foreach(println)
  }
}