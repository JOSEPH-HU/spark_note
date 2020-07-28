package com.joseph.core

import org.apache.spark.{SparkConf, SparkContext}

object TestRepartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestCoalesce").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val arr1 = List(("a",1),("a",2),("a",3),("b",2),("b",1),("c",4),("c",5),("a",9),("e",4),("f",4),("g",4),("g",4))
    /**
     * 用coalesce函数实现的 == coalesce(number,true)
     */
    val rdd = sc.parallelize(arr1,4).repartition(5)

    rdd.foreach(println)

    sc.stop()
  }

}
