package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform {
    //课时任务48
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)



        // TODO 算子 - flatMap
         val rdd = sc.makeRDD(List(List(1,2),List(3,4)))
        val rddFlat = rdd.flatMap(
            list =>{
                list
            }
        )
        rddFlat.collect().foreach(println)
        sc.stop()
    }
}
