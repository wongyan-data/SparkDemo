package com.travis.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //    todo  准备环境
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // todo   算子 -groupBy

    val rdd: RDD[String] = sc.makeRDD(

       List("hello" , "Spark","Scala", "hadoop")
    )
    val groupRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(
      _.charAt(0)
    )


    groupRDD.collect().foreach(println)





  //    todo  关闭资源
    sc.stop()

  }

}
