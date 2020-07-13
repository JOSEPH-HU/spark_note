package com.joseph.core

import org.apache.spark.{SparkConf, SparkContext}

object TestMapPartitions {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestMapPartitions").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val a = sc.parallelize(1 to 9, 3)
    def doubleFunc(iter: Iterator[Int]) : Iterator[(Int,Int)] = {
      var res = List[(Int,Int)]()
      while (iter.hasNext)
      {
        val cur = iter.next;
        res .::= (cur,cur*2)
      }
      res.iterator
    }
    val result = a.mapPartitions(doubleFunc)
    println(result.collect().mkString)

  }

}
