package com.atguigu.bigdata.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_IO_Load {
    def main(args: Array[String]): Unit = {
        //TODO 建立和Spark框架的连接
        // 类似于JDBC: Connection
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
        val sc = new SparkContext(sparkConf)

        val rdd1: RDD[String] = sc.textFile(path = "output1")
        println(rdd1.collect().mkString(","))

        val rdd2: RDD[String] = sc.objectFile[String](path = "output2")
        println(rdd2.collect().mkString(","))

        val rdd3: RDD[(String, Int)] = sc.sequenceFile[String,Int](path ="output3")
        println(rdd3.collect().mkString(","))

        sc.stop()
    }
}
