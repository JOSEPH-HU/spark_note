package com.joseph.core

import org.apache.spark.{SparkConf, SparkContext}

object TestAggregateByKey2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestAggregateByKey").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val list = List(("a",2),("a",3),("b",1),("c",1),("c",1),("c",2))

    //合并同一个分区的值，a是zerovalue的值的类型，b是value值的类型
    def seqOpt(a:(Int,Double,Double),b:Int):(Int,Double,Double)={
      println("seqopt" + "a=" + a + ";b=" + b)
      (a._1+1,0,a._3+b)
    }
  //合并不同分区的值，a和b时serovalue的值的类型
    def comOpt(a:(Int,Double,Double),b:(Int,Double,Double)):(Int,Double,Double) = {
      println("comOpt" + a + "-" + b)
      (a._1+b._1,a._3+b._3, (a._3+b._3)/(a._1+b._1))
    }

    val rdd = sc.parallelize(list,3).cache()

    def par(index:Int,iter:Iterator[(String,Int)]):Iterator[(String,Int)] = {
      println("[partID:" +  index + ", val: " + iter.toList + "]")
      iter
    }
   rdd.mapPartitionsWithIndex(par).collect()

    //zeroValue 是需要返回的格式
    val result = rdd.aggregateByKey((0,0D,0D))(seqOpt,comOpt)

    result.foreach(println)

    sc.stop()
}

}
