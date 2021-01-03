package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Operator_Transform {
    //课时任务66
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key - Value类型) groupBy

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a",1),("a",2),("a",3),("b",4)
        ))

        // groupByKey: 将数据源中的数据，相同key的数据分到一个组中，形成一个对偶元组
        //             元组中的第一个元素就是key
        //             元组中的第二个元素就是相同key的value的集合
        val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()

        groupRDD.collect().foreach(println)
        //打印结果
        //(a,CompactBuffer(1, 2, 3))
        //(b,CompactBuffer(4))

        //groupBy和groupByKey区别，看返回值类型
        val groupRDD1: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)

        groupRDD1.collect().foreach(println)
        //打印结果
        //(a,CompactBuffer((a,1), (a,2), (a,3)))
        //(b,CompactBuffer((b,4)))

        sc.stop()
    }
}