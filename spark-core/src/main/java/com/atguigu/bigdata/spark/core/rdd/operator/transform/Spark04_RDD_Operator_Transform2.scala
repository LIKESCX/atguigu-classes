package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform2 {
    //课时任务49
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)



        // TODO 算子 - flatMap

         val rdd = sc.makeRDD(List(List(1,2),3,List(4,5)))
        rdd
        val flatRDD = rdd.flatMap(
            data =>{
                data match {//模式匹配
                    case list:List[_] => list
                    case dat => List(dat)
                }
            }
        )
        flatRDD.collect().foreach(println)
        sc.stop()
    }
}
