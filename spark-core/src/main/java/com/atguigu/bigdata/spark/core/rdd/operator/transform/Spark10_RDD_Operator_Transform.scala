package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operator_Transform {
    //课时任务58
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - coalesce

        val rdd = sc.makeRDD(seq = List(1,2,3,4,5,6),numSlices = 3)

        // coalesce方法默认情况下不会将分区的数据打乱重新组合
        // 这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
        // 如果想要让数据均衡，可以进行shuffle处理
        //val coaleseRDD: RDD[Int] = rdd.coalesce(numPartitions = 2)
        val coaleseRDD: RDD[Int] = rdd.coalesce(numPartitions = 2,shuffle = true)

        coaleseRDD.saveAsTextFile("output")

        sc.stop()
    }
}
