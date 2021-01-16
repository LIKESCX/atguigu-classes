package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

//课时116
object Spark04_Req1_HotCategoryT10Analysis3 {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryT10Analysis");
        val sc = new SparkContext(sparkConf)

        // TODO: Top热门品类

        // Q: 之前的方法中存在大量的shuffle操作(reduceByKey)
        // reduceByKey 聚合算子，spark会提供优化，缓存

        //1. 读取原始日志数据
        val actionRDD: RDD[String] = sc.textFile(path = "datas/user_visit_action.txt")
        val hac =  new HotCategoryAccumulator

        sc.register(hac,"HotCategory")
        actionRDD.foreach(
            action ⇒{
                val datas: Array[String] = action.split("_")
                if(datas(6) !="-1"){
                    hac.add((datas(6),"click"))
                }else if(datas(8) !="null"){
                    val arr: Array[String] = datas(8).split(",")
                    arr.foreach(item ⇒{
                        hac.add((item,"order"))
                    })
                } else if(datas(10) !="null"){
                    val arr: Array[String] = datas(10).split(",")
                    arr.foreach(item ⇒hac.add((item,"pay"))
                  )
                }
            }
        )

        val hcMap: mutable.Map[String,HotCategory] = hac.value
        val categories: mutable.Iterable[HotCategory] = hcMap.map(_._2)
        categories.toList.sortWith(//自定义排序
            (left,right) ⇒{
            if(left.clickCnt > right.clickCnt){
                true
            }else if(left.clickCnt == right.clickCnt){
                if(left.orderCnt > right.orderCnt){
                    true
                }else if(left.orderCnt == right.orderCnt){
                    left.payCnt > right.payCnt
                }else{
                    false
                }
            }else{
                false
            }
        }).take(10).foreach(println)
        sc.stop()
    }

    case class HotCategory(cid:String, var clickCnt: Int, var orderCnt: Int ,var payCnt: Int)

    //1. 自定义累加器类，继承AccumulatorV2[IN,OUT] 确定泛型的类型
    //   IN     (品类ID,点击行为)
    //   OUT    (品类ID,(品类ID,(点击数量,下单数量,支付数量)))
    class HotCategoryAccumulator extends AccumulatorV2[(String,String),mutable.Map[String,HotCategory]]{
        private var hcMap = mutable.Map[String,HotCategory]()
        override def isZero: Boolean = {
            hcMap.isEmpty
        }

        override def copy(): AccumulatorV2[(String, String), mutable.Map[String,HotCategory]] = {
            new HotCategoryAccumulator
        }

        override def reset(): Unit = {
            hcMap.clear()
        }

        override def add(v: (String, String)): Unit = {
            val cid = v._1
            val actionType = v._2
            val category = hcMap.getOrElse(cid,HotCategory(cid,0,0,0))
            if(actionType =="click"){
                category.clickCnt += 1
            }else if(actionType =="order"){
                category.orderCnt += 1
            }else if(actionType =="pay"){
                category.payCnt += 1
            }
            hcMap.update(cid,category)
        }

        override def merge(other: AccumulatorV2[(String, String), mutable.Map[String,HotCategory]]): Unit = {
            var map1 = this.hcMap
            var map2 : mutable.Map[String,HotCategory] = other.value

            map2.foreach{
                case (cid,hc) ⇒ {
                    val category = map1.getOrElse(cid,HotCategory(cid,0,0,0))
                    category.clickCnt += hc.clickCnt
                    category.orderCnt += hc.orderCnt
                    category.payCnt += hc.payCnt
                    map1.update(cid,category)
                }
            }
        }

        override def value: mutable.Map[String,HotCategory] = {
            hcMap
        }
    }
}

//HotCategory(15,6120,1672,1259)
//HotCategory(2,6119,1767,1196)
//HotCategory(20,6098,1776,1244)
//HotCategory(12,6095,1740,1218)
//HotCategory(11,6093,1781,1202)
//HotCategory(17,6079,1752,1231)
//HotCategory(7,6074,1796,1252)
//HotCategory(9,6045,1736,1230)
//HotCategory(19,6044,1722,1158)
//HotCategory(13,6036,1781,1161)
