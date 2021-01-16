package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//课时114
object Spark02_Req1_HotCategoryT10Analysis1 {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryT10Analysis");
        val sc = new SparkContext(sparkConf)
        // Q: actionRDD 重复使用 通过cache解决
        // Q:cogroup 性能可能较低 通过union解决
        // TODO: Top热门品类
        //1. 读取原始日志数据
        val actionRDD: RDD[String] = sc.textFile(path = "datas/user_visit_action.txt")
        actionRDD.cache()
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

        // 另一种思路
        //(品类ID,点击数量) => (品类ID,(点击数量,0,0))
        //(品类ID,下单数量) => (品类ID,(0,下单数量,0))
        //                 => (品类ID,(点击数量,下单数量,0))
        //(品类ID,支付数量) => (品类ID,(0,0,支付数量))
        //                 => (品类ID,(点击数量,下单数量,支付数量))
        val newRDD1: RDD[(String, (Int, Int, Int))] = rdd1.mapValues { cnt ⇒ (cnt, 0, 0)
        }

        val newRDD2: RDD[(String, (Int, Int, Int))] = rdd2.mapValues { cnt ⇒ (0, cnt, 0)
        }

        val newRDD3: RDD[(String, (Int, Int, Int))] = rdd3.mapValues { cnt ⇒ (0, 0, cnt)
        }

        //5. 将品类进行排序，并且取前10名
        //   点击数量排序，下单数量排序，支付数量排序
        //   元组排序：先比较第一个，再比较第二个，再比较第三个，依次类推
        //   (品类ID,(点击数量,下单数量,支付数量))

        // cogroup = connect + group

        // cogroup 有可能会存在shuffle
        /*val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2,rdd3)
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
        }*/

        //union 并集要求两个数据源类型一致,不存在shuffle操作
        // 将三个数据源合并在一起，统一进行聚合计算
        val unionRDD: RDD[(String, (Int, Int, Int))] = newRDD1.union(newRDD2).union(newRDD3)
        val analysisRDD: RDD[(String, (Int, Int, Int))] = unionRDD.reduceByKey ({
            (t1, t2) ⇒ {
                (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
            }
        })

        val result: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2,false).take(10)
        // 6. 将结果采集到控制台打印出来
        result.foreach(println)
    }
}
