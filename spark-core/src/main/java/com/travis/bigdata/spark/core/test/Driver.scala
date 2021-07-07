package com.travis.bigdata.spark.core.test

import java.io.{ObjectOutputStream, OutputStream}
import java.net.{ServerSocket, Socket}

object Driver {

  def main(args: Array[String]): Unit = {


    val client1: Socket = new Socket("localhost",9999)
    val client2: Socket = new Socket("localhost",8888)


    val task = new Task()

    val out1 : OutputStream = client1.getOutputStream
    val ObjectOut1: ObjectOutputStream = new ObjectOutputStream(out1)
    val subtask1: SubTask = new SubTask()
    subtask1.logic = task.logic
    subtask1.data = task.data.take(2)
    ObjectOut1.writeObject(subtask1)
    ObjectOut1.flush()
    ObjectOut1.close()
    client1.close()


    val out2 : OutputStream = client2.getOutputStream
    val ObjectOut2: ObjectOutputStream = new ObjectOutputStream(out2)
    val subtask2: SubTask = new SubTask()
    subtask2.logic = task.logic
    subtask2.data = task.data.takeRight(2)
    ObjectOut2.writeObject(subtask2)
    ObjectOut2.flush()
    ObjectOut2.close()
    client1.close()








    println("数据发送完毕")
  }
}
