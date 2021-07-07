package com.travis.bigdata.spark.core.test

class Task extends Serializable {
  val data = List(1,2,3,4)
  val logic = (num : Int) => {num *2}
// val logic : (Int)=> Int = _ *2

}
