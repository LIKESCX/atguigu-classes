package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark23_RDD_Operator_Transform {
    //课时任务77
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key - Value类型) cogroup

        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
            ("a",1),("b",2),("c",3),("d",3)
        ))

        val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
            ("a",4),("b",5),("c",6),("d",7)
        ))

        val rdd3: RDD[(String, Int)] = sc.makeRDD(List(
            ("b",9),("c",10),("d",11)
        ))

        //cogroup: connect + group (分组 + 连接)
        //val cgRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
        //cgRDD.collect().foreach(println)


        val cgRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2,rdd3)
        cgRDD.collect().foreach(println)
        val analysisRDD: RDD[(String, (Int, Int, Int))] = cgRDD.mapValues { case (iter1, iter2, iter3) ⇒ {
            var c1 = 0;
            val iterator= iter1.iterator;
            if(iterator.hasNext) {
                c1 = iterator.next()
                println("c1="+c1)
            }
            var c2 = 0;
            if (iter2.iterator.hasNext) {
                c2 = iter2.iterator.next()
                println("c2="+c2)
            }
            var c3 = 0;
            if (iter3.iterator.hasNext) {
                c3 = iter3.iterator.next()
                println("c3="+c3)
            }
            (c1, c2, c3)
        }
        }
        analysisRDD.collect().foreach(println)
        println("***************")
        //val i2 = Iterable(6,7,8,9,10);
        //val iterator = i2.iterator
        //while(iterator.hasNext) {
        //    //next获取下一个数据
        //    print(iterator.next())
        //}
        sc.stop()
    }
}
