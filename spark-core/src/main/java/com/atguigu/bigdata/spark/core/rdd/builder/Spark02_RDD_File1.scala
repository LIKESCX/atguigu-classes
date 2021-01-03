package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File1 {

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

        //textFile: 以行为单位读取数据，读取的数据都是字符串
        // wholeTextFile: 以文件为单位读取数据
        //     读取的结果表示为元组，第一个元素表示文件路径，第二个元素及以后的为文件内容
        val rdd = sc.wholeTextFiles(path="datas")

        //收集整个RDD到到单节点上，并打印
        rdd.collect().foreach(println)

        // TODO 关闭环境
        sc.stop()
    }
}
