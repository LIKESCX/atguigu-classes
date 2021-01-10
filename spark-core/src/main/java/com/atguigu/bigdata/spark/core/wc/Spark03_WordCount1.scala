package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
//课时85和86
object Spark03_WordCount1 {
  def main(args: Array[String]): Unit = {

      //TODO 建立和Spark框架的连接
      val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
      val sc = new SparkContext(sparkConf)
      // TODO 执行业务操作
      //wordcount1(sc)
      //wordcount2(sc)
      //wordcount3(sc)
      //wordcount4(sc)
      //wordcount5(sc)
      //wordcount6(sc)
      //wordcount7(sc)
      //wordcount8(sc)
      //wordcount9(sc)
      //wordCount10(sc)
      wordCount11(sc)
      // TODO 关闭连接
      sc.stop();
  }

    //groupBy
    def wordcount1(sc: SparkContext): Unit = {

        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala","Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val groupRDD: RDD[(String, Iterable[String])] = words.groupBy(word ⇒ word)
        val wordCount1: RDD[(String, Int)] = groupRDD.mapValues(iter ⇒ iter.size)
        println("-----------groupBy------------")
        wordCount1.collect().foreach(println)

    }

    //groupByKey
    def wordcount2(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala","Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val mapRDD: RDD[(String, Int)] = words.map((_,1))
        val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
        val wordCount2: RDD[(String, Int)] = groupRDD.mapValues(iter ⇒ iter.size)
        println("-----------groupByKey------------")
        wordCount2.collect().foreach(println)

    }

    //reduceByKey
    def wordcount3(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala","Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val mapRDD: RDD[(String, Int)] = words.map((_,1))
        val wordCount3: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
        println("-----------reduceByKey------------")
        wordCount3.collect().foreach(println)

    }
    //aggregateByKey
    def wordcount4(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala","Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val mapRDD: RDD[(String, Int)] = words.map((_,1))
        val wordCount4: RDD[(String, Int)] = mapRDD.aggregateByKey(zeroValue = 0)(_+_,_+_)
        println("-----------aggregateByKey------------")
        wordCount4.collect().foreach(println)
    }
    //foldByKey
    def wordcount5(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala","Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val mapRDD: RDD[(String, Int)] = words.map((_,1))
        val wordCount5: RDD[(String, Int)] = mapRDD.foldByKey(zeroValue = 0)(_+_)
        println("-----------foldByKey------------")
        wordCount5.collect().foreach(println)
    }
    //combineByKey
    def wordcount6(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala","Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val mapRDD: RDD[(String, Int)] = words.map((_,1))
        val wordCount6: RDD[(String, Int)] = mapRDD.combineByKey (
            v ⇒ v,
            (x: Int, y) ⇒ x + y,
            (x: Int, y: Int) ⇒ x + y
        )
        println("-----------combineByKey------------")
        wordCount6.collect().foreach(println)
    }

    //countByValue
    def wordcount7(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala","Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        //val mapRDD: RDD[(String, Int)] = words.map((_,1))
        val wordCount7: collection.Map[String, Long] = words.countByValue()
        println("-----------countByValue------------")
        wordCount7.toList.foreach(println)
        //println(wordCount7.mkString(","))
    }

    //countByKey
    def wordcount8(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala","Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val mapRDD: RDD[(String, Int)] = words.map((_,1))
        val wordCount8: collection.Map[String, Long] = mapRDD.countByKey()
        println("-----------countByKey------------")
        val list: List[(String, Long)] = wordCount8.toList
        list.foreach(println)
    }

    //reduce
    def wordcount9(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala","Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val mapWord: RDD[mutable.Map[String, Long]] = words.map(
            word ⇒ {
                mutable.Map[String, Long]((word, 1))

            }
        )
        //mapWord.foreach(println)
        val wordCount9= mapWord.reduce(
            (map1, map2) ⇒ {
                map2.foreach{
                    case(word,count) ⇒{
                        val newCount = map1.getOrElse(word,0L) + count
                        map1.update(word,newCount)
                    }
                }
                map1
            }
        )

       println(wordCount9)
    }

    //aggregate
    def wordCount10(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala","Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val mapWord: RDD[mutable.Map[String, Long]] = words.map(
            word ⇒ {
                mutable.Map[String, Long]((word, 1))

            }
        )
        //mapWord.foreach(println)
        val wordCount10 = mapWord.aggregate(mutable.Map[String, Long](("", 0)))(
            (map1,map2) ⇒{
                map2.foreach{
                    case(word,count) ⇒{
                        val newCount = map1.getOrElse(word,0L) + count
                        map1.update(word,newCount)
                    }
                }
                map1
            },
            (map1,map2) ⇒{
                map2.foreach{
                    case(word,count) ⇒{
                        val newCount = map1.getOrElse(word,0L) + count
                        map1.update(word,newCount)
                    }
                }
                map1
            }
        )
        wordCount10.remove("")
        println(wordCount10)
    }
    //fold
    def wordCount11(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala","Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val mapWord: RDD[mutable.Map[String, Long]] = words.map(
            word ⇒ {
                mutable.Map[String, Long]((word, 1))

            }
        )
        //mapWord.foreach(println)
        val wordCount11 = mapWord.fold(mutable.Map[String, Long](("", 0)))(
            (map1,map2) ⇒{
                map2.foreach{
                    case(word,count) ⇒{
                        val newCount = map1.getOrElse(word,0L) + count
                        map1.update(word,newCount)
                    }
                }
                map1
            }
        )
        wordCount11.remove("")
        println(wordCount11)
    }

}
