package com.travis.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    //    TODO 简历和Spark的连接

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(sparkConf)

    //    TODO 执行业务炒作

    //    1.读取文件，获取一行一行的文件
    //    hello scala
    val fileRDD: RDD[String] = sc.textFile("data")

    //    2、 数据分割
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))

    //    3.将相同的数据放到一起,转化数据结构（world world ）-》 （world ，2）
    val word2OneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))

    //        4.相同的单词进行聚合
    val word2CountRDD: RDD[(String, Int)] = word2OneRDD.reduceByKey(_ + _)

    //    5.将聚合结果存储到内存中
    val wordCount : Array[(String, Int)] = word2CountRDD.collect()
    wordCount.foreach(println)


//    TODO 关闭连接
    sc.stop()
  }
}
