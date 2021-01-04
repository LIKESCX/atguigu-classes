package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Operator_Transform3 {
    //课时任务73
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key - Value类型) combineByKey

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a",1),("a",2),("b",3),
            ("b",4),("b",5),("a",6)
        ),numSlices = 2)

        //combineByKey: 方法需要三个参数
        // 第一个参数表示：将相同key的第一个数据进行结构的转换，实现操作
        // 第二个参数表示：分区内的计算规则
        // 第三个参数表示：分区间的计算规则

        //获取相同key的数据平均值=>(a,3),(b,4)
        val aggRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
            v =>(v,1),// 1 => (1,1)
            (t:(Int,Int), v) => { // (1,1),2 => (3,1)
            println(s"t = ${t}, v = ${v}")
            (t._1 + v, t._2 + 1)
        },
        (t1, t2) => {
            println(s"t1 = ${t1}, t2 = ${t2}")
            (t1._1 + t2._1, t1._2 + t2._2)
        })

        aggRDD.collect().foreach(println)

        val mapRDD: RDD[(String, Int)] = aggRDD.mapValues {
            case (num, cnt) => {//模式匹配
                num / cnt
            }
        }

        mapRDD.collect().foreach(println)

        sc.stop()
    }
}
