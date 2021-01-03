package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
      //Application
      //Spark框架

      //TODO 建立和Spark框架的连接
      // 类似于JDBC: Connection
      val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
      val sc = new SparkContext(sparkConf)
      // TODO 执行业务操作
      // 1. 读取文件，获取一行一行的数据
      // hello world
      val lines: RDD[String] = sc.textFile("datas")
      // 2. 将一行数据进行拆分，形成一个一个的单词（分词）
      // "hello world" => hello, world, hello, world
      //  扁平化: 将整体拆分成个体的操作
      val words: RDD[String] = lines.flatMap(_.split(" "))

      val wordToOne = words.map{
        word => (word,1)
      }

      val wordGroup: RDD[(String,Iterable[(String,Int)])] =  wordToOne.groupBy(
        tuple => tuple._1
      )

      val wordToCount =  wordGroup.map{
        case (word,list) =>{
          list.reduce(//reduce将RDD中元素前两个传给输入函数，产生一个新的return值，将新产生的return值与RDD中下一个元素（即第三个元素）组成两个元素，再被传给输入函数，这样递归运作，直到最后只有一个值为止
            (t1,t2) => {
              (t1._1,t1._2 + t2._2)
            }
          )
        }
      }
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
