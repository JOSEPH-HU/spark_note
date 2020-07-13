package com.joseph.core

import org.apache.spark.{SparkConf, SparkContext}

object TestDistinct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestDistinct").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val arr = Array(1,2,3,4,5,5,6,3)
    val rdd = sc.parallelize(arr).distinct(2)

    rdd.foreach(println)
  }

}
