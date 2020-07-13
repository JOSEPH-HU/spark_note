package com.joseph.core

import org.apache.spark.{SparkConf, SparkContext}

object TestMapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestMapPartitionsWithIndex").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val arr = Array(1,2,3,4,6)
    val rdd = sc.parallelize(arr,3).mapPartitionsWithIndex((index,itr)=>{
      var res = List[(Int,Int)]()
      while (itr.hasNext){
        res .::= (index,itr.next())
      }
      res.iterator
    })
    rdd.foreach((line=>println(line._1 + "-" + line._2)))
  }

}
