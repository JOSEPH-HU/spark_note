package com.joseph.core

import org.apache.spark.{SparkConf, SparkContext}

object TestAggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestAggregateByKey").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val list = List(("a",2),("a",3),("b",1),("c",1),("c",1),("c",2))

    //合并同一个分区的值，a是zerovalue的值的类型，b是value值的类型
    def seqOpt(a:Int,b:Int):Int={
      println("seqopt" + "a=" + a + ";b=" + b)
      a+b
    }
  //合并不同分区的值，a和b时value的值
    def comOpt(a:Int,b:Int):Int = {
      println("comOpt" + a + "-" + b)
      a*b
    }

    val rdd = sc.parallelize(list,3).cache()

    def par(index:Int,iter:Iterator[(String,Int)]):Iterator[(String,Int)] = {
      println("[partID:" +  index + ", val: " + iter.toList + "]")
      iter
    }
   // rdd.mapPartitionsWithIndex(par).collect()

    val result = rdd.aggregateByKey(10)(seqOpt,comOpt)

    result.foreach(println)
    sc.stop()

}

}
