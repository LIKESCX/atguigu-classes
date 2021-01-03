package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform {
    //课时任务63 和 64
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key - Value类型)
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
        val mapRDD: RDD[(Int, Int)] = rdd.map((_,1))
        // RDD => PairRDDFunctions
        // 隐式转换(二次编译)

        // partitionBy根据指定的分区规则对数据进行重分区
        mapRDD.partitionBy(new HashPartitioner(partitions = 2))
                    .saveAsTextFile("output")
        sc.stop()
    }
}
