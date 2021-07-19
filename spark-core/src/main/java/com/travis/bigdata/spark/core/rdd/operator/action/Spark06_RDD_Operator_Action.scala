package com.travis.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Action {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc: SparkContext = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4)
    )

    //    TODO - 行动算子


    val user: user = new user
    rdd.foreach(
      num => {
        println("age = " + (user.age + num))
      }
    )


    sc.stop()

  }


  class user extends Serializable {
    var age : Int = 30
  }
}
