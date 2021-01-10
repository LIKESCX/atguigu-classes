package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//课时102
object Spark06_RDD_Persist {
    def main(args: Array[String]): Unit = {

        // cache: 将数据临时存储到内存中进行数据重用
        //        会在血缘关系中添加新的依赖。一旦，出现问题，可以重头读取数据
        //persist:  将数据临时存储在磁盘文件中进行数据重用
        //          涉及到磁盘IO操作，性能较低，但是数据安全
        //          如果作业执行完毕，临时保存的数据文件就会丢失
        //checkpoint: 将数据长久的保存在磁盘文件中进行数据重用
        //            涉及到磁盘IO操作，性能较低，但是数据安全
        //            为了保证数据安全，所以一般情况下，会独立执行作业
        //            为了能够提高效率，一般情况下，是需要和cache联合使用
        //            执行过程中会切断血缘关系。重写建立新的血缘关系
        //            checkpoint等同于改变数据源

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

        //mapRDD.cache()
        mapRDD.checkpoint()
        println(mapRDD.toDebugString)
        val wordCount: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
        wordCount.collect().foreach(println)
        println("**********************************")
        println(mapRDD.toDebugString)
        val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

        groupRDD.collect().foreach(println)


        sc.stop()
    }
}
