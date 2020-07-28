package com.joseph.core

import org.apache.spark.{SparkConf, SparkContext}

object TestFlatMapValues {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestJoin").setMaster("local[1]")
    val sc = new SparkContext(conf)


    val arr2 = List(("a","1"),("a","2"),("b","2"),("d","4"))

    val rdd2 = sc.parallelize(arr2)

    rdd2.flatMapValues(f=> f + "1").foreach(println)

    sc.stop()
  }

}
