package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

//任务109
object Spark06_Bc {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Acc");
        val sc = new SparkContext(sparkConf)

        val rdd1 = sc.makeRDD(List(("a",1),("b",2),("c",3),("d",4)))

        val map = mutable.Map(("b",3),("c",2),("d",1))

        //封装广播变量
        val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

        rdd1.map{
            case (w,c) ⇒{
                //访问广播变量
                val l: Int = bc.value.getOrElse(w,0)
                (w,(c,l))
            }
        }.collect().foreach(println)

        sc.stop()
    }
}
