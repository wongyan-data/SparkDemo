package com.travis.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {


    //    todo  准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //    todo  创建RDD
    //    从文件中创建RDD  ，将文件的路径做为处理的数据源  可以使用通配符*进行模式识别匹配
    val rdd: RDD[String] = sc.textFile("data/1.txt")
//    val rdd = sc.wholeTextFiles("data")  可以知道来自于哪个文件
    rdd.collect().foreach(println)

    //    todo  关闭资源
    sc.stop()
  }

}
