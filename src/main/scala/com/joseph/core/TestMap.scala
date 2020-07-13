package com.joseph.core

import org.apache.spark.{SparkConf, SparkContext}

object TestMap {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestMap").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val arr = List(1,2,3)

    val rdd = sc.parallelize(arr)

    rdd.foreach(println)

  }

}
