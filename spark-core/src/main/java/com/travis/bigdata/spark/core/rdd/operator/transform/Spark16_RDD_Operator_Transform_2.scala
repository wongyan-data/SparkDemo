package com.travis.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Operator_Transform_2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf  = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //    todo   算子 -aggregateByKey2
    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(("a", 2), ("a", 3), ("a", 2),("a",5)),2
    )

    /**
     *     aggregate存在函数的科里化    两个参数列表
     *              第一个参数列表，需要传递一个参数，表示初始值
     *                      主要是当碰到第一个key的时候，和value进行分区内计算
     *              第二个参数列表需要传递两个阐述
     *                      第一个参数表示分区内的计算规则
     *                      第二个参数表示分区间的计算规则                          元组中的第二个元素是value的集合
     */

    val newRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }


    )

    val value: RDD[(String, Int)] = newRDD.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }


    value.collect(
    ).foreach(println)


    //    todo  关闭资源
    sc.stop()

  }
}
