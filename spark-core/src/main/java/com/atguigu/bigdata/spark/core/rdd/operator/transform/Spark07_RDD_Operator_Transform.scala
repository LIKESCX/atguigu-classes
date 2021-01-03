package com.atguigu.bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Transform {
    //课时任务55
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - filter

        val rdd = sc.makeRDD(List(1,2,3,4),2)
        //rdd.filter(num => num % 2 != 0)
        val filterRDD = rdd.filter(_ % 2 != 0)
        filterRDD.collect.foreach(println)
        sc.stop()
    }
}
