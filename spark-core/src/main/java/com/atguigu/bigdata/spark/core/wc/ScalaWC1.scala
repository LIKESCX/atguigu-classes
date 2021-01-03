package com.atguigu.bigdata.spark.core.wc

object ScalaWC1 {
  def main(args: Array[String]): Unit = {
    val list = List(List("rose","is","beautiful"),List("jennie","is","beautiful"),List("lisa","is","beautiful"),List("jisoo","is","beautiful"));
    val newList = list.flatMap(list2 =>{
      list2
    })
    println(newList)
    //打印结果
    //List(rose, is, beautiful, jennie, is, beautiful, lisa, is, beautiful, jisoo, is, beautiful)

//    val list = List("rose is beautiful","jennie is beautiful","lisa is beautiful","jisoo is beautiful")
//    /**
//      * 第一步，将list中的元素按照分隔符这里是空格拆分，然后展开
//      * 先map(_.split(" "))将每一个元素按照空格拆分
//      * 然后flatten展开
//      * flatmap即为上面两个步骤的整合
//      */
//    val res0= list.map(_.split(" "))//没有执行flatten时res0的数据集合如下：
//    //List(List("rose","is","beautiful"),List("jennie","is","beautiful"),List("lisa","is","beautiful"),List("jisoo","is","beautiful"))
//    val res0_1 = res0.flatten
//    val res1= list.flatMap(_.split(" ")) // res0_1的结果和res1的一样如下：
//    //List(rose, is, beautiful, jennie, is, beautiful, lisa, is, beautiful, jisoo, is, beautiful)
//    println("第一步结果")
//    println(res0_1)
//    //res0(2).foreach(println)
//    println(res1)
//
//    println("第二步结果")
//    val wordGroup = res1.groupBy(wd => wd)//返回的是一个Map
//    println(wordGroup)
//    //打印结果为：
//    //Map(
//    // beautiful -> List(b eautiful, beautiful, beautiful, beautiful),
//    // is -> List(is, is, is, is),
//    // lisa -> List(lisa),
//    // jennie -> List(jennie),
//    // jisoo -> List(jisoo),
//    // rose -> List(rose)
//    // )
//    println("第三步结果")
//    val wordCount = wordGroup.map {
//      case (word,list) => {
//        (word,list.size)
//      }
//    }
//    println(wordCount)
//    //打印结果为：
//    //Map(beautiful -> 4, is -> 4, lisa -> 1, jennie -> 1, jisoo -> 1, rose -> 1)
//    println("第四步结果")
//    val arrays = wordCount.toArray
//    arrays.foreach(println)
//
//    val wordToOne = res1.map{
//      word =>(word,1)// 遍历取出res1中的每个元素进行加工变成(word,1)元组的形式
//    }
//    println(wordToOne)
//    //打印结果 List中每个元素是一个个元组。
//    //List((rose,1), (is,1), (beautiful,1), (jennie,1), (is,1), (beautiful,1), (lisa,1), (is,1), (beautiful,1), (jisoo,1), (is,1), (beautiful,1))
//    val wordGroup1 = wordToOne.groupBy(
//      tuple => tuple._1
//    )
//    println(wordGroup1)
//    //打印结果
////    Map(beautiful -> List((beautiful,1), (beautiful,1), (beautiful,1), (beautiful,1)), is ->        List((is,1), (is,1), (is,1), (is,1)), lisa -> List((lisa,1)), jennie -> List((jennie,1)),         jisoo -> List((jisoo,1)), rose -> List((rose,1)))
//    val wordToCount = wordGroup1.map{
//        case(word,list) =>{
//            list.reduce(//聚合
//               (tuple1,tuple2) => {
//                 (tuple1._1, tuple1._2 + tuple2._2)
//               }
//            )
//        }
//    }
//    println(wordToCount)
    //打印结果
//    Map(beautiful -> 4, is -> 4, lisa -> 1, jennie -> 1, jisoo -> 1, rose -> 1)
  }
}
