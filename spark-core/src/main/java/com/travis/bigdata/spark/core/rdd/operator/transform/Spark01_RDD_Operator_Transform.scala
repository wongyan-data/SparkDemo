package com.travis.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //    todo  准备环境
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // todo   算子 -map
    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4)
    )
    def mapFunction(num:Int) :Int = {
      num*2
    }

//    val mapRDD: RDD[Int] = rdd.map(mapFunction)
      val mapRDD: RDD[Int] = rdd.map(
        (num:Int)=>{
          num*2
        }
      )
//          等价于  val mapRDD: RDD[Int] = rdd.map(_*2)

    mapRDD.collect().foreach(println)
  //    todo  关闭资源
    sc.stop()
//    178.191.44.227 - - 17/05/2015:12:05:05 +0000 GET /projects/xdotool/xdotool.xhtml
  }

}
