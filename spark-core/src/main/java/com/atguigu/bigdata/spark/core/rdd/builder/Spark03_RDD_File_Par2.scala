package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_File_Par2 {
    // 对应课时39
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

        // 14byte / 2 = 7byte
        // 14byte / 7 = 2(个分区)

        /*
            1234567@@   =>  0123456789
            89@@        =>  10111213
            0           =>  14

            0 =>[0,7]   =>1234567
            1 =>[7,14]  =>890
         */
        // 如果数据源为多个文件，那么计算分区时以文件为单位进行分区
        val rdd = sc.textFile(path = "datas/word.txt",minPartitions = 2)
        rdd.saveAsTextFile("output")
        // TODO 关闭环境
        sc.stop()
    }
}
