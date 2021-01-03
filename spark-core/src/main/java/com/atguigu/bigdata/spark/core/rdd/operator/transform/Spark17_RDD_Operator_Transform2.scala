package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform2 {
    //课时任务70
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key - Value类型) foldByKey

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a",1),("a",2),("a",3),("b",5),
            ("b",1),("b",2),("b",3),("a",6)
        ),numSlices = 2)

        //rdd.aggregateByKey(zeroValue = 0)(_+_,_+_).collect().foreach(println)

        //如果聚合计算时，分区内和分区间计算规则相同时，spark提供了简化的方法

        rdd.foldByKey(zeroValue = 0)(_+_).collect().foreach(println)
        sc.stop()
    }
}
