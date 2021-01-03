package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_Par1 {
    // 对应课时38
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
        // TODO 数据分区的分配
        // 1. 数以行为单位进行读取
        // spark读取文件，采用的是hadoop的方式读取，所以一行一行读取，和字节数没有关系
        // 2. 数据读取时以偏移量为单位，偏移量不会被重复读取
        /*
            数据  偏移量
            1@@ =>012
            2@@ =>345
            3   =>6
         */
        // 3. 数据分区的偏移量范围计算
        //分区号 预估偏移量范围      每个分区实际存储数据
        // 0 =>     [0,3]          =>1,2
        // 1 =>     [3,6]          =>3
        // 2 =>     [6,7]          =>无
        val rdd = sc.textFile("datas/1.txt",2)
        rdd.saveAsTextFile("output")
        // TODO 关闭环境
        sc.stop()
    }
}
