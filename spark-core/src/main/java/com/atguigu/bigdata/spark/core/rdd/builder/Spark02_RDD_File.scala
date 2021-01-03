package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {

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
        // 从文件中创建RDD,将文件中的数据作为处理的数据源
        /**
          * path路径默认以当前环境的根路径为基准，可以写绝对路径，也可以写相对路径
          * path路径可以是文件的具体路径，也可以是目录名称
          * path路径还可以使用通配符*
          * path路径还可以是分布式存储系统：如HDFS
          *
          */
        //val rdd = sc.textFile(path="F:\\atguigu-classes\\datas\\1.txt")
        //val rdd = sc.textFile(path="datas/1.txt")
        //val rdd = sc.textFile(path="datas")
        //val rdd = sc.textFile(path="datas/1*.txt")
        val rdd = sc.textFile(path="hdfs://linux1:8020/test.txt")
        //收集整个RDD到到单节点上，并打印
        rdd.collect().foreach(println)

        // TODO 关闭环境
        sc.stop()
    }
}
