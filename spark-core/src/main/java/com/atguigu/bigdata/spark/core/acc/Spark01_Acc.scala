package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

//任务105
object Spark01_Acc {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Acc");
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4))

        val sum1: Int = rdd.reduce(_+_)
        println("sum1="+sum1)

        var sum2 = 0

        rdd.foreach(
            num ⇒{
                sum2 += num
            }
        )
        println("sum2="+sum2)

        sc.stop()
    }
}
