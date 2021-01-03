package com.atguigu.bigdata.spark.core.test

import java.io.ObjectOutputStream
import java.net.Socket

object Driver {

    def main(args: Array[String]): Unit = {

        //创建客户端发送请求
        val client = new Socket("localhost",9999)

        val out = client.getOutputStream()
        val outOjb = new ObjectOutputStream(out)
        val task = new Task()
        outOjb.writeObject(task)
        outOjb.flush()

        outOjb.close()
        out.close()
        client.close()
    }
}
