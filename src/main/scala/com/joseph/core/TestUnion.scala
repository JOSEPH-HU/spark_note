package com.joseph.core

import org.apache.spark.{SparkConf, SparkContext}

object TestUnion {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestUnion").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val arr1 = Array(1,2,3,4)
    val arr2 = Array(3,4,5,6)

    val rdd1 = sc.parallelize(arr1)
    val rdd2 = sc.parallelize(arr2)

    val rdd = rdd1.union(rdd2)
    rdd.foreach(println)

    sc.stop()
  }

}
