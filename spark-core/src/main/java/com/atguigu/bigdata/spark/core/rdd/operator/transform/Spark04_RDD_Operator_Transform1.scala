package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform1 {
    //课时任务48
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)



        // TODO 算子 - flatMap
         val rdd = sc.makeRDD(List("Hello Scala","Hello Spark"))
        rdd
        val rddFlat = rdd.flatMap(
            s =>{
                s.split(" ")
            }
        )
        rddFlat.collect().foreach(println)
        sc.stop()
    }
}
