package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//课时122
object Spark06_Req3_PageflowAnalysis {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryT10Analysis");
        val sc = new SparkContext(sparkConf)

        // TODO: Top热门品类 --每个品类的 Top10 活跃 Session 统计
        //1. 读取原始日志数据
        val actionRDD: RDD[String] = sc.textFile(path = "datas/user_visit_action.txt")
        //actionRDD.cache()
        val pageIdList = List[Long](1,2,3,4,5,6,7)
        val okflowIds: List[(Long, Long)] = pageIdList.zip(pageIdList.tail)

        //计算分母
        val uvaRDD: RDD[UserVisitAction] = actionRDD.map {
            action ⇒ {
            val datas = action.split("_")
                new UserVisitAction(datas(0), datas(1).toLong, datas(2), datas(3).toLong, datas(4), datas(5), datas(6).toLong, datas(7).toLong, datas(8), datas(9), datas(10), datas(11), datas(12).toLong)
            }
        }

        val fenmus: Map[Long, Long] = uvaRDD.filter(
            item ⇒{
                pageIdList.init.contains(item.page_id)
            }
        ).map {
            item ⇒ {
                (item.page_id, 1L)
            }
        }.reduceByKey(_ + _).collect().toMap

        uvaRDD.cache()


        //计算分子

        //根据session进行分组
        val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = uvaRDD.groupBy(_.session_id)

        //分组后，根据访问时间进行排序(升序)
        val mvRDD: RDD[(String, List[(Long, Long)])] = sessionRDD.mapValues { iter ⇒ {
            val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)

            //[1,2,3,4]
            //[1,2] [2,3] [3,4]
            //[1-2] [2-3] [3-4]
            // Sliding 和zip
            val flowIds: List[Long] = sortList.map(_.page_id)
            val pageFlowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)//List.tail 表示取的是尾部信息（舍弃第一个元素后的）
            pageFlowIds.filter(
                t ⇒{
                    okflowIds.contains(t)
                }
            )
        }
        }
        val fenziRDD: RDD[((Long, Long), Long)] = mvRDD.flatMap {
            item ⇒ {
                item._2
            }
        }.map((_, 1L)).reduceByKey(_ + _)

        fenziRDD.foreach{
            case ((pageId1,pageId2),sum) ⇒{
                val l: Long = fenmus.getOrElse(pageId1,0L)
                println(s"从页面${pageId1}跳到页面${pageId2}的单跳转换率是"+sum.toDouble/l)
            }
        }
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

//从页面2跳到页面3的单跳转换率是0.019949423995504357
//从页面4跳到页面5的单跳转换率是0.018323153803442533
//从页面1跳到页面2的单跳转换率是0.01510989010989011
//从页面3跳到页面4的单跳转换率是0.016884531590413945
//从页面5跳到页面6的单跳转换率是0.014594442885209093
//从页面6跳到页面7的单跳转换率是0.0192040077929307
