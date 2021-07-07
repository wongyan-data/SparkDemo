package com.travis.bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform_test {
  def main(args: Array[String]): Unit = {
    //    todo  准备环境
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // todo   算子 -groupBy

    val rdd:RDD[String] = sc.textFile("data/apache.log")

    val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(
      line => {
        val data: Array[String] = line.split(" ")
        val time: String = data(3)
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val data1: Date = sdf.parse(time)
        val sdf1: SimpleDateFormat = new SimpleDateFormat("HH")
        val hour: String = sdf1.format(data1)
        (hour, 1)
      }
    ).groupBy(_._1)


    timeRDD.map{
      case (hour ,iter) => {
        (hour ,iter.size)
    }
    }.collect().foreach(println)



  //    todo  关闭资源
    sc.stop()

  }

}
