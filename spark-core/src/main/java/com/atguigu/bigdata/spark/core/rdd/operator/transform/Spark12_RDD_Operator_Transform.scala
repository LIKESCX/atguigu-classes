package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform {
    //课时任务60
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - sortBy

        val rdd = sc.makeRDD(seq = List(6,2,5,4,1,3),numSlices = 2)

        val newRDD: RDD[Int] = rdd.sortBy(num => num)

        newRDD.saveAsTextFile("output")

        sc.stop()
    }
}
