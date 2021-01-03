package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par1 {

    def main(args: Array[String]): Unit = {

        // TODO 准备环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        //sparkConf.set("spark.default.parallelism","3")
        // sc：指的是上下文环境对象
        val sc = new SparkContext(sparkConf)

        // TODO 创建RDD
        // 【1,2】【3,4】
        //val rdd = sc.makeRDD(List(1,2,3,4),2)
        // 【1】【2】【3,4】
        //val rdd = sc.makeRDD(List(1,2,3,4),3)
        // 【1】 【2,3】【4,5】
        //val rdd = sc.makeRDD(List(1,2,3,4,5),3)
        //【1,2,3】 【4,5,6】 【7,8,9,0】
        val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,0),3)

        // 将处理的数据保存成分区文件
        rdd.saveAsTextFile("output")
        // TODO 关闭环境
        sc.stop()

        //分析一下，如何把集合内存中的数据放到指定分区的。
        /*val array = seq.toArray // To prevent O(n^2) operations for List etc
       positions(array.length, numSlices).map { case (start, end) =>
           array.slice(start, end).toSeq
       }.toSeq*/


        /*def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
            (0 until numSlices).iterator.map { i =>
                val start = ((i * length) / numSlices).toInt
                val end = (((i + 1) * length) / numSlices).toInt
                (start, end)
            }
        }*/

        /*override def slice(from: Int, until: Int): Array[T] = {

        }*/
        /**
          *  length = 10 numSlices = 3
          *  1,2,3,4,5,6,7,8,9,0
          *  0  => (0,3)  =>(1,2,3)
          *  1  => (3,6)  =>(4,5,6)
          *  2  => (6,10) =>(7,8,9,10)
          *
          */



    }
}
