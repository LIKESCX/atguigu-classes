package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_Par {
    // 对应课时37
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
        // textFile可以将文件作为数据处理的数据源，默认也可以设定分区。
        //      minPartitions: 最小分区数量,并不是实际的分区数量
        //      math.min(defaultParallelism, 2)
        //val rdd = sc.textFile("datas/1.txt")
        // 如果不想使用默认的分区数量，也可以通过第二个参数指定分区数
        // Spark读取文件，底层其实使用的就是Hadoop的读取方式
        // 分区数量的计算方式：
        // totalSize = 7  // compute total size
        // goalSize = 7 /2 = 3(byte) 表示每个分区存几个字节
            //long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits); // numSplits 就是 minPartitions
        // 实际分区数: 7 / 3 = 2...1 (1.1) +1 = 3个(分区) //1子节是3子节的33.3%,大于了hadoop切片的1.1倍的策略,因此会多创建一个分区

        val rdd = sc.textFile(path="datas/1.txt",minPartitions =2)
        rdd.saveAsTextFile("output")
        // TODO 关闭环境
        sc.stop()
    }
}
