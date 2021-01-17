package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//课时118
object Spark05_Req2_HotCategoryT10Analysis {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryT10Analysis");
        val sc = new SparkContext(sparkConf)

        // TODO: Top热门品类 --每个品类的 Top10 活跃 Session 统计
        //1. 读取原始日志数据
        val actionRDD: RDD[String] = sc.textFile(path = "datas/user_visit_action.txt")


        val ids: Array[Long] = getHotCategoryIds(actionRDD)
        //ids.foreach(println)

        val uvaRDD: RDD[UserVisitAction] = actionRDD.map(action ⇒ {
            val datas: Array[String] = action.split("_")
            new UserVisitAction(datas(0), datas(1).toLong, datas(2), datas(3).toLong, datas(4), datas(5), datas(6).toLong, datas(7).toLong, datas(8), datas(9), datas(10), datas(11), datas(12).toLong)
        })

        //((品类ID,SessionID),1)
        val reduceRDD: RDD[((Long, String), Int)] = uvaRDD.filter(
            item ⇒ {
            ids.contains(item.click_category_id) && item.search_keyword !="-1"
        }).map(item ⇒ {
            ((item.click_category_id, item.session_id), 1)
        }).reduceByKey(_ + _) //((品类ID,SessionID),1) =>((品类ID,SessionID),sum)
        //reduceRDD.collect().foreach(println)
        //转换数据结构((品类ID,SessionID),sum) => (品类ID,(SessionID,sum))
        val mapRDD: RDD[(Long, (String, Int))] = reduceRDD.map {
            case ((cid, sessoinId), cnt) ⇒ {
                (cid, (sessoinId, cnt))
            }
        }

        //将同一品类ID的数据放到一个组中
        val groupRDD: RDD[(Long, Iterable[(String, Int)])] = mapRDD.groupByKey()

        val resultRDD: RDD[(Long, List[(String, Int)])] = groupRDD.mapValues(iter ⇒ {
            iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
        })
        resultRDD.collect().foreach(println)


        // 6. 将结果采集到控制台打印出来
        //result.foreach(println)
    }

    // 获取 Top10的品类ID集合
    def getHotCategoryIds(actionRDD: RDD[String]): Array[Long] = {

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


        val newRDD1: RDD[(String, (Int, Int, Int))] = rdd1.mapValues { cnt ⇒ (cnt, 0, 0)
        }

        val newRDD2: RDD[(String, (Int, Int, Int))] = rdd2.mapValues { cnt ⇒ (0, cnt, 0)
        }

        val newRDD3: RDD[(String, (Int, Int, Int))] = rdd3.mapValues { cnt ⇒ (0, 0, cnt)
        }


        val unionRDD: RDD[(String, (Int, Int, Int))] = newRDD1.union(newRDD2).union(newRDD3)
        val analysisRDD: RDD[(String, (Int, Int, Int))] = unionRDD.reduceByKey ({
            (t1, t2) ⇒ {
                (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
            }
        })
        analysisRDD.sortBy(_._2,false).take(10).map(_._1.toLong)
    }

    //用户访问动作表
    case class UserVisitAction(
              date: String,//用户点击行为的日期
              user_id: Long,//用户的 ID
              session_id: String,//Session 的 ID
              page_id: Long,//某个页面的 ID
              action_time: String,//动作的时间点
              search_keyword: String,//用户搜索的关键词
              click_category_id: Long,//某一个商品品类的 ID
              click_product_id: Long,//某一个商品的 ID
              order_category_ids: String,//一次订单中所有品类的 ID 集合
              order_product_ids: String,//一次订单中所有商品的 ID 集合
              pay_category_ids: String,//一次支付中所有品类的 ID 集合
              pay_product_ids: String,//一次支付中所有商品的 ID 集合
              city_id: Long //城市 id
    )
}
