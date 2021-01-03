package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform1 {
    //课时任务69
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key - Value类型) aggregateByKey

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a",1),("a",2),("a",3),("b",6),
            ("b",1),("b",2),("b",3),("a",6)
        ),numSlices = 2)


        rdd.aggregateByKey(zeroValue = 5)(
            (x,y) => {
                println(s"x = ${x}, y = ${y}")
                math.max(x,y)
            },
            (x,y) => x + y
        ).collect().foreach(println)

        sc.stop()
    }
}
