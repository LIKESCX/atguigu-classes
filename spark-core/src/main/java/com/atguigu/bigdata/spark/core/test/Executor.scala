package com.atguigu.bigdata.spark.core.test

import java.io.ObjectInputStream
import java.net.ServerSocket

object Executor {

    def main(args: Array[String]): Unit = {

        //启动服务器接收数据
        val server = new ServerSocket(9999)

        println("开启服务器，等待接收请求!")
        while(true){
            //等待客户端的连接
            val clinet = server.accept()

            val in = clinet.getInputStream()
            val objIn = new ObjectInputStream(in)
//            val i: Int = in.read()
            val task: Task = objIn.readObject().asInstanceOf[Task]
            val ints = task.compute()
            println("计算节点计算的结果:"+ints)
            in.close()
            clinet.close()
        }
        //server.close()
    }
}
