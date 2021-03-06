package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

// 课时 83
object Spark03_RDD_Operator_Action {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4),numSlices = 2)

        // TODO - 行动算子 aggregate , fold
        // 10 + 13 + 17 = 40
        //aggregateByKey: 初始值只会参与分区内计算
        //aggregate: 初始值会参与分区内计算，并且也参与分区间计算
        //val result: Int = rdd.aggregate(zeroValue = 10)(_+_,_+_)
        val result: Int = rdd.fold(zeroValue = 10)(_+_)
        println(result)

        sc.stop()
    }
}
