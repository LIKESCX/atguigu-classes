package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

//课时100
object Spark03_RDD_Persist {
    def main(args: Array[String]): Unit = {
        //TODO 建立和Spark框架的连接
        // 类似于JDBC: Connection
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.makeRDD(List("hello world","hello spark","hive","atguigu"))

        val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

        val mapRDD: RDD[(String, Int)] = flatRDD.map{
            word ⇒ {
                println("@@@@@@@@@@@@@@@@@@@@@@@@@@@")
                (word,1)
            }
        }
        //cache默认持久化的操作，只能保存将数据保存到内存中，如果想要保存到磁盘文件（为临时文件，执行完后会自动删除，所以这里不指定保存路径），需要更改存储级别
        //mapRDD.cache()
        //持久化操作必须在行动算子执行时完成的。
        mapRDD.persist(StorageLevel.DISK_ONLY)

        val wordCount: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
        wordCount.collect().foreach(println)
        println("**********************************")

        val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

        groupRDD.collect().foreach(println)


        sc.stop()
    }
}
