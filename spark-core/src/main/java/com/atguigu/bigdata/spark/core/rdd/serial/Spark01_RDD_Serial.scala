package com.atguigu.bigdata.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Serial {
    def main(args: Array[String]): Unit = {
        //TODO 建立和Spark框架的连接
        // 类似于JDBC: Connection
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.makeRDD(Array("hello world","hello spark","hive","atguigu"))

        val search = new Search(query = "h")

        //search.getMatch1(rdd).collect().foreach(println)
        search.getMatch2(rdd).collect().foreach(println)

        sc.stop()
    }

    //查询对象
    // 类的构造参数其实是类的属性,构造参数需要进行闭包检测，其实就等同于类进行闭包检测
    class Search(query: String) extends  Serializable{
        def isMatch(s: String): Boolean = {
            //s.contains(this.query)
            s.contains(query)//这里的query 为this.query的省略
        }

        //函数序列化案例
        def getMatch1(rdd:RDD[String]): RDD[String] = {
            rdd.filter(isMatch)
        }

        //属性序列化案例

        def getMatch2(rdd: RDD[String]): RDD[String] = {
            val s = query
            rdd.filter(x ⇒ x.contains(s))
        }
    }

    /**
      * 这里解决方案有三种：
      *  1. Search 类 继承 Serializable
      *  2. case class Search
      *  3. 将 query 赋值给一个局部变量s.然将s传递给rdd
      *
    */
}
