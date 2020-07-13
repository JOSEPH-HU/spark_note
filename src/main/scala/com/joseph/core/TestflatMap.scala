package com.joseph.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * flatmap时输入一行输出多行
 * map时输入一行输出一行
 *
 * 这是两个转换的区别
 */
object TestflatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestflatMap").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val arr = Array("1,2,3,","9,4,5,")
    val rdd = sc.parallelize(arr).flatMap(_.split(","))
    rdd.foreach(println)

  }

}
