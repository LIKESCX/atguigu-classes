package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

// 课时87
object Spark05_RDD_Operator_Action {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(("a",1),("b",2),("c",3),("d",4))
            ,numSlices = 2)

        // TODO - 行动算子 save相关的算子

        rdd.saveAsTextFile("output")
        rdd.saveAsObjectFile("output1")
        // saveAsSequenceFile方法要求数据格式必须为K-V类型
        rdd.saveAsSequenceFile("output2")

        sc.stop()
    }
}
