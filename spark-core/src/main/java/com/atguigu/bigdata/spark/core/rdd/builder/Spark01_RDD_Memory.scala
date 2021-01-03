package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {

    def main(args: Array[String]): Unit = {

        // TODO 准备环境
        // [*]:表示当前本机的最大核数是多少，如果是8核，这里会启动8个线程来模拟真实场景
        // 如果local后面不加[*],则是模拟单核，单线程的场景
        //[2]:将*写成数字则是指定一定数量的核数来模拟场景。
        // appName:指的是应用程序的名称
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        // sc：指的是上下文环境对象
        val sc = new SparkContext(sparkConf)

        // TODO 创建RDD
        // 从内存中创建RDD,将内存中集合的数据作为处理的数据源
        val seq = Seq[Int](1,2,3,4)

        // parallelize: 并行
        //val rdd: RDD[Int] = sc.parallelize(seq) //方式一

        // makeRDD方法在底层实现时其实就是调用了rdd对象的parallelize方法。
        // 两个方法本质其实都一样，只是makeRDD见名知意，比parallelize 易懂。
        val rdd: RDD[Int] = sc.makeRDD(seq) // 方式二

        //收集整个RDD到到单节点上，并打印
        rdd.collect().foreach(println)

        // TODO 关闭环境
        sc.stop()
    }
}
