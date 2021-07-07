package com.travis.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //    todo  准备环境
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // todo   算子 -filter

    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4, 5,6,7,8,9,10)
    )
// simple算子需要传递的参数
//    1.第一个参数表示，抽取数据后是否将数据返回true（放回），false（丢弃）
//    2.表示被抽取的概率
//    3.表示抽取数据时的随机算法的种子    不传递就是当地系统时间

    println(rdd.sample(
      true,
      0.4,
//      1
    ).collect().mkString(","))





  //    todo  关闭资源
    sc.stop()

  }

}
