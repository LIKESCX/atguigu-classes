package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_Transform {
    //课时任务59
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - repartition

        val rdd = sc.makeRDD(seq = List(1,2,3,4,5,6),numSlices = 2)

        // coalesce算子可以扩大分区的，但是如果不进行shuffle操作，是没有意义的，不起作用。
        // 所以如果想要实现扩大分区的效果，需要使用shuffle操作
        // spark提供了一个简化的操作
        // 缩减分区：coalesce,如果想要数据均衡，可以采用shuffle
        // 扩大分区：repartition,底层代码调用的就是coalesce，而且肯定采用了shuffle
        //val newRDD: RDD[Int] = rdd.coalesce(numPartitions = 3,shuffle = true)
        val newRDD: RDD[Int] = rdd.repartition(numPartitions = 2)
        newRDD.saveAsTextFile("output")

        sc.stop()
    }
}
