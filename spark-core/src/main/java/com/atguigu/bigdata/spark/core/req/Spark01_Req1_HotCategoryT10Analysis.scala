package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//课时112 和113
object Spark01_Req1_HotCategoryT10Analysis {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryT10Analysis");
        val sc = new SparkContext(sparkConf)
        // TODO: Top热门品类
        //1. 读取原始日志数据
        val actionRDD: RDD[String] = sc.textFile(path = "datas/user_visit_action.txt")

        //2. 统计品类的点击数量：（品类ID,点击数量）
        val clickRDD: RDD[String] = actionRDD.filter{
            action ⇒ {
                val datas: Array[String] = action.split("_")
                datas(6) !="-1"
            }
        }
        val rdd1: RDD[(String, Int)] = clickRDD.map {
            action ⇒ {
                val datas: Array[String] = action.split("_")
                (datas(6), 1)
            }
        }.reduceByKey(_ + _)
        //rdd1.collect().foreach(println)

        //3. 统计品类的下单数量：（品类ID,下单数量）
        val orderRDD: RDD[String] = actionRDD.filter{
            action ⇒ {
                val datas: Array[String] = action.split("_")
                datas(8) !="null"
            }
        }
        val rdd2: RDD[(String, Int)] = orderRDD.flatMap{
            action ⇒ {
                val datas: Array[String] = action.split("_")
                val datas1: Array[String] = datas(8).split(",")
                datas1.map((_,1))
            }
        }.reduceByKey(_ + _)
        //4. 统计品类的支付数量：（品类ID,支付数量）
        val payRDD: RDD[String] = actionRDD.filter{
            action ⇒ {
                val datas: Array[String] = action.split("_")
                datas(10) !="null"
            }
        }
        val rdd3: RDD[(String, Int)] = payRDD.flatMap{
            action ⇒ {
                val datas: Array[String] = action.split("_")
                val datas1: Array[String] = datas(10).split(",")
                datas1.map((_,1))
            }
        }.reduceByKey(_ + _)
        //5. 将品类进行排序，并且取前10名
        //   点击数量排序，下单数量排序，支付数量排序
        //   元组排序：先比较第一个，再比较第二个，再比较第三个，依次类推
        //   (品类ID,(点击数量,下单数量,支付数量))
        val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2,rdd3)
        val analysisRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues {
            case (iter1, iter2, iter3) ⇒ {
                var clickCount = 0
                val iterator1 = iter1.iterator
                if (iterator1.hasNext) {
                    clickCount = iterator1.next()
                }

                var orderCount = 0
                val iterator2 = iter2.iterator
                if (iterator2.hasNext) {
                    orderCount = iterator2.next()
                }

                var payCount = 0
                val iterator3 = iter3.iterator
                if (iterator3.hasNext) {
                    payCount = iterator3.next()
                }
                (clickCount, orderCount, payCount)
            }
        }

        val result: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2,false).take(10)
        // 6. 将结果采集到控制台打印出来
        result.foreach(println)
    }
}
