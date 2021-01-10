package com.atguigu.bigdata.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_IO_Save {
    def main(args: Array[String]): Unit = {
        //TODO 建立和Spark框架的连接
        // 类似于JDBC: Connection
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3)))

        rdd.saveAsTextFile(path = "output1")
        rdd.saveAsObjectFile(path = "output2")
        rdd1.saveAsSequenceFile(path="output3")

        sc.stop()
    }
}
