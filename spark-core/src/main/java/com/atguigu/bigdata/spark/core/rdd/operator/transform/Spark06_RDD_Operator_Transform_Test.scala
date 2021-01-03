package com.atguigu.bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform_Test {
    //课时任务54
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - groupBy 小练习
        val rdd = sc.textFile("datas/apache.log")
        val timeRDD = rdd.map({
            line =>{
                val datas = line.split(" ")
                val time = datas(3)
                val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                val date = sdf.parse(time)
                val sdf1 = new SimpleDateFormat("HH")
                val hour = sdf1.format(date)
                (hour,1)
            }
        }).groupBy(_._1)

        val hourRDD = timeRDD.map{
            //模式匹配
            case (hour,iter) =>{
                (hour,iter.size)
            }
        }.collect.foreach(println)

        //hourRDD.collect().foreach(println)

        sc.stop()
    }
}
