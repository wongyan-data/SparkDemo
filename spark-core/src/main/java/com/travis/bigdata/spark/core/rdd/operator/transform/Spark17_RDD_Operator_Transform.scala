package com.travis.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf  = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //    todo   算子 -GroupBykey
    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(("a", 2), ("a", 4), ("c", 2))
    )

    /**
     *     相同的key的数据分在一个组中  ， 形成一个对偶元祖
     *                          元组中的第一个元素是Key
     *                          元组中的第二个元素是value的集合
      */

    val rdd1: RDD[(String, Iterable[Int])] = rdd.groupByKey()



    rdd.groupBy(_._1)
    rdd1.collect().foreach(println)

    //    todo  关闭资源
    sc.stop()

  }
}
