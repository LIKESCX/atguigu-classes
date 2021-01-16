package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayOps

//课时115
object Spark03_Req1_HotCategoryT10Analysis2 {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryT10Analysis");
        val sc = new SparkContext(sparkConf)

        // TODO: Top热门品类

        // Q: 之前的方法中存在大量的shuffle操作(reduceByKey)
        // reduceByKey 聚合算子，spark会提供优化，缓存

        //1. 读取原始日志数据
        val actionRDD: RDD[String] = sc.textFile(path = "datas/user_visit_action.txt")

        //2. 将数据转换结构
        // 点击的场合：(品类ID,(1,0,0))
        // 下单的场合：(品类ID,(0,1,0))
        // 支付的场合：(品类ID,(0,0,1))
        val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
            action ⇒ {
                val datas: Array[String] = action.split("_")
                // datas(6) !="-1"   点击的场合
                // datas(8) !="null" 下单的场合
                // datas(10) !="null" 支付的场合aa
                if (datas(6) != "-1") {
                    //datas.map(datas(6),(1,0,0))
                    List((datas(6), (1, 0, 0)))
                } else if (datas(8) != "null") {
                    val datas1: Array[String] = datas(8).split(",")
                    datas1.map((_, (0, 1, 0)))
                } else if (datas(10) != "null") {
                    val datas2: Array[String] = datas(10).split(",")
                    datas2.map((_, (0, 0, 1)))
                } else {
                    //Nil  //即Nil是空List  Nil == List()
                    List()
                }

            }
        )

        //3. 将相同的品类ID的数据进行分组聚合
        // (品类ID,(点击数量,下单数量,支付数量))
        val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey((t1, t2) ⇒ {
            (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
        })
        //4. 将统计结果根据数量进行降序处理，取前10名
        val result: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2,false).take(10)
        // 5. 将结果采集到控制台打印出来
        result.foreach(println)
        //result.foreach(item ⇒{
        //    val a = item._1.getClass
        //    println(a)
        //})

        sc.stop()
    }
}
