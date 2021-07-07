package com.travis.bigdata.spark.core.test

class SubTask extends Serializable {


  var data :List[Int]=_

  var logic : (Int) =>Int = _
  // val logic : (Int)=> Int = _ *2

  def computer () = {
    data.map(logic)
  }
}
