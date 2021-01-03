package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform_Test {
    //课时任务45
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)



        // TODO 算子 - mapPartitions 小练习
        val rdd = sc.makeRDD(List(1,2,3,4),2)

        //求出每个分区的最大值
        //【1,2】  【3,4】
        // 【2】    【4】
        val mpRDD = rdd.mapPartitions(
            iter =>{
                List(iter.max).iterator
            }
        )
        mpRDD.collect().foreach(println)
        sc.stop()
    }
}
