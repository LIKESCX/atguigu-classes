package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

//任务106
object Spark03_Acc {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Acc");
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4))

        //获取系统累加器
        //Spark默认就提供了简单数据聚合的累加器
        val sumAcc: LongAccumulator = sc.longAccumulator(name = "sum")

        val mapRDD = rdd.map(
            num ⇒{
                //使用累加器
                sumAcc.add(num)
                num
            }
        )

        //获取累加器的值
        //少加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
        //多加：转换算子中调用累加器，如果多次调用行动算子，就会出现多加现象。
        mapRDD.collect()
        mapRDD.collect()
        println(sumAcc.value)
        sc.stop()
    }
}
