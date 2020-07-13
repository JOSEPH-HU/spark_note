package com.joseph.core

import org.apache.spark.{SparkConf, SparkContext}

object TestIntersection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestIntersection").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val arr1 = Array(1,2,3,4)
    val arr2 = Array(3,4,5,6)

    val rdd1 = sc.parallelize(arr1,1)
    val rdd2 = sc.parallelize(arr2,1)
  //两个RDD的交集
    val rdd = rdd1.intersection(rdd2,2)

    rdd.foreach(println)
  }

}
