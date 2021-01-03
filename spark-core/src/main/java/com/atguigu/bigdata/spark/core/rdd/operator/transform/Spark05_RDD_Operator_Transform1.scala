package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform1 {
    //课时任务50
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - glom 小功能 求出每个分区的最大值，并计算出最大值之和

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

        val glomRDD: RDD[Array[Int]] = rdd.glom()

        val maxRDD: RDD[Int] = glomRDD.map({
            array => array.max
        })
        println(maxRDD.collect().mkString(","))//打印出最大值

        println(maxRDD.collect.sum)//计算最大值之和
        sc.stop()
    }
}
