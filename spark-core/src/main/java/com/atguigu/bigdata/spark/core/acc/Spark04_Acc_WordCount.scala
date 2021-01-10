package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

//任务107和108
object Spark04_Acc_WordCount {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Acc");
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List("hello","spark","hello"))

        //累加器：实现wordcount
        //创建累加器对象
        val wcAcc = new MyAccumulator
        //向Spark进行注册
        sc.register(wcAcc, name="wordCountAcc")

        rdd.foreach(
            word ⇒{
                //数据的累加（使用累加器）
                wcAcc.add(word)
            }
        )
        //获取累加器累加的结果
        println(wcAcc.value)
        sc.stop()
    }

    /**
      * 自定义累加器
      *
      * 1. 继承 AccumulatorV2，定义泛型
      *
      * IN: 累加器输入的数据类型 String
      * OUT: 累加器返回的数据类型 mutable.Map[String, Long]
      *
      * 2. 重写方法（6）
      */

    class MyAccumulator extends  AccumulatorV2[String,mutable.Map[String,Long]]{

        private var wcMap = mutable.Map[String,Long]()

        //判断是否为初始状态
        override def isZero: Boolean = {
            wcMap.isEmpty
        }

        override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
            new MyAccumulator
        }

        override def reset(): Unit = {
            wcMap.clear()
        }
        //获取累加器需要计算的值
        override def add(word: String): Unit = {
            val newCount = wcMap.getOrElse(word,0L) + 1
            wcMap.update(word,newCount)
        }

        // Driver 合并多个累加器
        override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
            val map1 = this.wcMap
            val map2 = other.value

            map2.foreach{
                case (word,count) ⇒{
                    println("map1==="+map1)
                    val newCount = map1.getOrElse(word,0L) + count
                    map1.update(word,newCount)
                }
            }


        }

        override def value: mutable.Map[String, Long] = {
            wcMap
        }
    }
}
