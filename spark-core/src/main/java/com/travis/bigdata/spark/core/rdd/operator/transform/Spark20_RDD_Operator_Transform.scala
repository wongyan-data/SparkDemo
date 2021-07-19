package com.travis.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 */
object Spark20_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf  = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //    todo   算子 -combineByKey
    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(("a", 2), ("a", 4), ("c", 2),
        ("b", 2), ("b", 4), ("b", 2)
      ),numSlices = 2
    )


    /**
     * reduceByKey:
     *          combineByKeyWithClassTag[V](
     *          (v: V) =>v,   // 第一个值不会参与计算
     *          func,         // 分区内计算规则
     *          func,         // 分区间计算规则
     *          partitioner)
     *aggtegateByKey:
     *          combineByKeyWithClassTag[U]((v: V) =>
     *          cleanedSeqOp(createZero(), v),  // 初始化和第一个Key的value进行的操作
     *          cleanedSeqOp,                   // 分区内操作
     *          combOp,                         // 分区间操作
     *          partitioner)
     *foldByKey:
     *          combineByKeyWithClassTag[V](
     *          (v: V) =>cleanedFunc(createZero(), v),
     *            cleanedFunc,
     *            cleanedFunc,
     *            partitioner)
     * combinedByKey:
     *            combineByKeyWithClassTag(
     *            createCombiner,      // 相同key的第一条数据进行的处理函数
     *            mergeValue,          // 分区内的处理函数
     *            mergeCombiners,     // 分区间的处理函数
     *            defaultPartitioner(self))
     */

    rdd.reduceByKey(_ + _)  // wordcount
    rdd.aggregateByKey(0)(_+_ , _ + _)  // wordcount
    rdd.foldByKey(0)( _ +  _)   // wordcount
    rdd.combineByKey(v=> v , (x: Int , y ) => {x+ y} , (x: Int , y : Int) => x + y )   // wordcount



    //    todo  关闭资源
    sc.stop()

  }
}
