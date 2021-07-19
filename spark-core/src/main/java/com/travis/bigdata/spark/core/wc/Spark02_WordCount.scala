package com.travis.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    //    TODO 简历和Spark的连接

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(sparkConf)

    //    TODO 执行业务炒作

    wordCount10(sc)


//    TODO 关闭连接
    sc.stop()
  }

//groupBy
  def  wordCount1(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val group : RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordCount : RDD[(String ,Int)]= group.mapValues(iter => iter.size)
  }

  //groupByKey   效率不高 有shuffle
  def  wordCount2(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val group: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
    val wordCount : RDD[(String ,Int)]= group.mapValues(iter => iter.size)
  }

  //reduceByKey   效率不高 有shuffle
  def  wordCount3(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount = wordOne.reduceByKey(_ + _)

  }

  //groupByKey
  def  wordCount4(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount = wordOne.reduceByKey(_ + _)
  }


  //aggregateByKey
  def  wordCount5(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount = wordOne.aggregateByKey(0)(_ + _ , _ + _)
  }

  //foldByKey 分区内和分区间相同的方法
  def  wordCount6(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount = wordOne.foldByKey(0)(_ + _  )
  }

  //foldByKey 分区内和分区间相同的方法
  def  wordCount7(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount = wordOne.combineByKey(
      v => v ,
      (x : Int , y )  =>{ x+ y},
      (x1: Int , y1:Int) => {x1 + y1}
    )
  }


  //countByKey 分区内和分区间相同的方法
  def  wordCount8(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount = wordOne.countByKey()

  }

  //countByValue 分区内和分区间相同的方法
  def  wordCount9(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val stringToLong: collection.Map[String, Long] = words.countByValue()

  }


  //countByValue 分区内和分区间相同的方法
  def  wordCount10(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapWord = words.map(
      word => {
        mutable.Map[String, Long]((word, 1))
      }
    )

    val wordCount: mutable.Map[String, Long] = mapWord.reduce(
      (map1, map2) => {
        map2.foreach {
          case (word, count) => {
            val newCount = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
          }

        }
        map1
      }
    )
    println(wordCount)
  }

}
