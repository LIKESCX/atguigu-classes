package com.atguigu.bigdata.spark.core.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

//课时103
object Spark01_RDD_Part {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[(String, String)] = sc.makeRDD(
            List(
                ("nba", "xxxxxxx"),
                ("cba", "xxxxxxx"),
                ("wnba", "xxxxxxx"),
                ("nba", "xxxxxxx")
            ))


        val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)//使用自定义的分区器对数据进行分区
        partRDD.saveAsTextFile(path = "output")
        sc.stop()
    }

    /**
      *自定义分区器
      *1. 继承Partitioner
      *2. 重写方法
      *
      *
     */
    class MyPartitioner extends  Partitioner{
        //分区数量
        override def numPartitions: Int = 3

        //根据数据的key值返回数据所在的分区索引（从0开始）
        override def getPartition(key: Any): Int = {
            key match {
                case "nba" ⇒ 0
                case "wnba" ⇒ 1
                case _ ⇒ 2
            }
        }
    }
}
