package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

//课时101
object Spark04_RDD_Persist {
    def main(args: Array[String]): Unit = {
        //TODO 建立和Spark框架的连接
        // 类似于JDBC: Connection
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
        val sc = new SparkContext(sparkConf)
        sc.setCheckpointDir("cp")


        val rdd: RDD[String] = sc.makeRDD(List("hello world","hello spark"))

        val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

        val mapRDD: RDD[(String, Int)] = flatRDD.map{
            word ⇒ {
                println("@@@@@@@@@@@@@@@@@@@@@@@@@@@")
                (word,1)
            }
        }

        //  checkpoint 需要落盘，需要指定检查点保存路径
        //  检查点路径保存的文件，当作业执行完毕后，不会被删除
        //  一般保存路径都是分布式存储系统：如HDFS
        mapRDD.checkpoint()

        val wordCount: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
        wordCount.collect().foreach(println)
        println("**********************************")

        val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

        groupRDD.collect().foreach(println)


        sc.stop()
    }
}
