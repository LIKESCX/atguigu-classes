package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Operator_Transform3 {
    //课时任务71
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key - Value类型) aggregateByKey 小练习

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a",1),("a",2),("b",3),
            ("b",4),("b",5),("a",6)
        ),numSlices = 2)

        // aggregateByKey最终的返回数据结果应该和初始值的类型保持一致
        //val aggRDD: RDD[(String, String)] = rdd.aggregateByKey(zeroValue = "")(_+_,_+_)

        //获取相同key的数据平均值=>(a,3),(b,4)
        val aggRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))((t, v) => {
            // t = (0,0)  v =1
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
