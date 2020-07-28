package com.joseph.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestSortByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestSortByKey").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val arr = List(("a",1),("a",2),("a",3),("b",2),("b",1),("c",4),("c",5),("a",9))
    val rdd = sc.parallelize(arr,2)
    //这个按照key进行排序
    rdd.sortByKey(true,2).foreach(println)


    sc.stop()
  }

}
