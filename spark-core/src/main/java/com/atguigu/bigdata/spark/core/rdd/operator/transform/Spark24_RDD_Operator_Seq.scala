package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Operator_Seq {
    //课时任务79 和 80
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 案例实操

        // 1. 获取原始数据：时间戳，省份，城市，用户，广告
        val rdd1: RDD[String] = sc.textFile("datas/agent.log")
        // 2. 将原始数据进行结构的转换。方便统计
        rdd1.saveAsTextFile(path = "output1")
        // 时间戳，省份，城市，用户，广告

        // =>
        // ((省份,广告),1)
        //val rdd2: RDD[Array[String]] = rdd1.map(_.split(" "))
        val rdd2: RDD[((String, String), Int)] = rdd1.map(
            line => {
                val datas: Array[String] = line.split(" ")
                ((datas(1), datas(4)), 1)
            }
        )


        //3. 将转换结构后的数据，进行分组聚合
        // ((省份,广告),1) => ((省份,广告),sum)
        //val rdd4: RDD[((String, String), Int)] = rdd3.foldByKey(0)(_+_)
        val rdd3: RDD[((String, String), Int)] = rdd2.reduceByKey(_+_)
        rdd3.saveAsTextFile(path = "output3")
        // 4. 将聚合后得数据进行结构的转换
        // ((省份,广告),sum) => (省份,(广告,sum))
        val rdd4: RDD[(String, (String, Int))] = rdd3.map {
            /*case (t, v) => {
                (t._1, (t._2, v))
            }*/
            case ((prv,ad),sum) => {//和上面的方法功能相同
                (prv,(ad,sum))
            }
        }

        // 5. 将转换结构后的数据根据省份进行分组
        // (省份,(广告,sum)) => (省份,[(广告A,sum),(广告B,sum)])
        val rdd5: RDD[(String, Iterable[(String, Int)])] = rdd4.groupByKey()

        // 6. 将分组后的数据组内排序（降序），取前三名
        val rdd6: RDD[(String, List[(String, Int)])] = rdd5.mapValues(
            iter => {
                iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
            }
        )
        rdd6.saveAsTextFile(path = "output6")
        // 7.收集并控制台打印
        rdd6.collect().foreach(println)

        sc.stop()
    }
}
