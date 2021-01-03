package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {
    //课时任务41
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4))

        // TODO 算子 - map

        // 1,2,3,4
        // 2,4,6,8

        //转换函数
        //def mapFuncation(num: Int) : Int = {
        //    num * 2
        //}
        //val mapRDD = rdd.map(mapFuncation)

        //val mapRDD = rdd.map((num:Int)=>{num*2})
        //val mapRDD = rdd.map((num:Int)=>num*2)
        //val mapRDD = rdd.map((num)=>num*2)
        //val mapRDD = rdd.map(num=>num*2)
        val mapRDD = rdd.map(_*2)//最终简化版本

        mapRDD.collect.foreach(println)

        sc.stop()
    }
}
