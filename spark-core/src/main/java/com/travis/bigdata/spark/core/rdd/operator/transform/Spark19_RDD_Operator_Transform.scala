package com.travis.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 */
object Spark19_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf  = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //    todo   算子 -combineByKey
    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(("a", 2), ("a", 4), ("c", 2),
        ("b", 2), ("b", 4), ("b", 2)
      ),numSlices = 2
    )
    //    第一个参数   将相同的Key的第一个数据进行结构转换，实现操作
    //    第二个参数   分区内的计算规则
    //    第三个参数   分区间的计算规则


    val newRDD: RDD[(String , (Int , Int))] = rdd.combineByKey(

      v =>   (v , 1) ,
      (t: (Int, Int) ,v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    //    todo  关闭资源
    sc.stop()

  }
}
