package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
      //Application
      //Spark框架

      //TODO 建立和Spark框架的连接
      val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
      val sc = new SparkContext(sparkConf)
      // TODO 执行业务操作
      val lines: RDD[String] = sc.textFile("datas")

      val words: RDD[String] = lines.flatMap(_.split(" "))

      val wordToOne: RDD[(String,Int)]  = words.map{
        word => (word,1)
      }

      // Spark 框架提供了更多的功能，可以将分组和聚合使用一个方法实现
      // reduceByKey: 相同的key的数据，可以对value进行聚合
      //1. wordToOne.reduceByKey((x, y) => {x + y})
      //2. wordToOne.reduceByKey((x, y) => x + y)//函数体只有一条的话{}可以省略
      val wordToCount = wordToOne.reduceByKey(_ + _)//函数中的参数只使用一次的话，可用_代替 ,和1.和2.的功能一样

      // 5. 将转换结果采集到控制台打印出来
      val array: Array[(String,Int)] = wordToCount.collect();
    /*collect: 收集一个弹性分布式数据集的所有元素到一个数组中,这样便于我们观察，毕竟分布式数据集比较抽象。
      Spark的collect方法，是Action类型的一个算子，会从远程集群拉取数据到driver端。最后，将大量数据
      汇集到一个driver节点上，将数据用数组存放，占用了jvm堆内存，非常容易造成内存溢出，只用作小型数据的观察
     */
      array.foreach(println)
      // TODO 关闭连接
      sc.stop();
  }
}
