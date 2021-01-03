package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform {
    //课时任务47
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)



        // TODO 算子 - mapPartitionsWithIndex
        val rdd = sc.makeRDD(List(1,2,3,4),2)
        // 【1,2】 【3,4】
        val mapRDD = rdd.mapPartitionsWithIndex(
            (index,iter) => {//index:分区号
                if(index == 1){
                    iter
                }else {
                    Nil.iterator
                }
            }
        )

        mapRDD.collect.foreach(println)
        sc.stop()
    }
}
