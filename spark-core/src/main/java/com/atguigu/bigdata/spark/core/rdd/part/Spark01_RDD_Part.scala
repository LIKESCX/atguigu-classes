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


        val newRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)
        newRDD.saveAsTextFile(path = "output")
        sc.stop()
    }

    /**
      *
      *
      *
      *
     */
    class MyPartitioner extends  Partitioner{

        override def numPartitions: Int = 3

        override def getPartition(key: Any): Int = {
            key match {
                case "nba" ⇒ 0
                case "wnba" ⇒ 1
                case _ ⇒ 2
            }
        }
    }
}
