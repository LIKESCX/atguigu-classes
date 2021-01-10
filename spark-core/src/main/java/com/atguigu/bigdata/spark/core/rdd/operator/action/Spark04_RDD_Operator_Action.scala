package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

// 课时 84
object Spark04_RDD_Operator_Action {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        //val rdd = sc.makeRDD(List(1,2,3,4),numSlices = 2)

        // TODO - 行动算子 countByValue
        // 统计value出现的次数
        //val intToLong: collection.Map[Int, Long] = rdd.countByValue()
        //println(intToLong)

        // TODO - 行动算子 countByValue
        // 统计key出现的次数和value无关
        val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)),numSlices = 2)
        val stringToLong: collection.Map[String, Long] = rdd.countByKey()
        println(stringToLong)
        sc.stop()
    }
}
