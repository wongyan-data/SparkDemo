package com.travis.bigdata.spark.core.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor2 {
  def main(args: Array[String]): Unit = {
    //      启动服务器
    val server = new ServerSocket(8888)
    println("等待客户端连接")
    //    等待客户连接
    val client: Socket = server.accept()

    val in: InputStream = client.getInputStream
    val ObjIn: ObjectInputStream = new ObjectInputStream(in)
    val task: SubTask = ObjIn.readObject().asInstanceOf[SubTask]
    val ints: List[Any] = task.computer()

    println("接受到数据")
    println("{9999}计算的结果为"+  ints)
    in.close()
    client.close()
    server.close()
  }
}
