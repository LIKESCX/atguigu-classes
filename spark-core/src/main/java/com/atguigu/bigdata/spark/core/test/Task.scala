package com.atguigu.bigdata.spark.core.test

class Task extends Serializable{//网络中发送对象必须序列化

    val  datas = List(1,2,3,4)

    def logic = (num: Int) => num * 2
    def compute(): List[Int] = {

        /*datas.map{
            i => i*2
        }*/
        datas.map(logic)
    }

}
