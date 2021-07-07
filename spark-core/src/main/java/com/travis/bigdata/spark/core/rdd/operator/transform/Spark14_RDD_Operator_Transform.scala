package com.travis.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf  = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //    todo   算子 -ReduceByKey
    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(("a", 2), ("a", 4), ("c", 2))
    )

//    相同的key的数据进行value数据的聚合
val reduceRDD = rdd.reduceByKey((x: Int, y: Int) => {
  x + y
})



    reduceRDD.collect().foreach(println)




    //    todo  关闭资源
    sc.stop()

  }
}
