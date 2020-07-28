package com.joseph.core

import org.apache.spark.{SparkConf, SparkContext}


object TestJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestJoin").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val arr1 = List(("a",1),("a",2),("a",3),("b",2),("b",1),("c",4),("c",5),("a",9))

    val arr2 = List(("a",1),("b",2),("d",4))

    val rdd1 = sc.parallelize(arr1)
    val rdd2 = sc.parallelize(arr2)

    //底层都是cogroup实现

    //交集
    val result = rdd1.join(rdd2)
    result.foreach(println)

    val result1 = rdd1.leftOuterJoin(rdd2).cache()
    result1.foreach(println)

    result1.filter(line=>line._2._2!=None).foreach(println)

    rdd1.fullOuterJoin(rdd2).foreach(println)

    sc.stop()



  }

}
