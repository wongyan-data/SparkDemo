package com.travis.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Action {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc: SparkContext = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4)
    )

//    TODO - 行动算子
    /**
     *      所谓的行动算子，其实就是触发作业（Job）执行的方法
     *      调用的是底层的runjob
     *      底层代码中会创建ActiveJob
     */


    rdd.collect()

sc.stop()

  }
}
