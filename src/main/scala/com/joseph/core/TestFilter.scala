package com.joseph.core

import org.apache.spark.{SparkConf, SparkContext}

object TestFilter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestFilter").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val arr = Array(1,2,3,4,5,7)

    val rdd = sc.parallelize(arr)
    val filterrdd = rdd.filter(line=>line>4)
    filterrdd.foreach(println)

    sc.stop()
  }

}
