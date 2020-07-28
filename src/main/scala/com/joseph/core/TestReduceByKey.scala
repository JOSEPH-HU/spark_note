package com.joseph.core

import org.apache.spark.{SparkConf, SparkContext}

object TestReduceByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestReduceByKey").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val arr = Array(1,2,3,4,4,4,4,5,6,6,6,7,7)
    /**
     * 在计算和和平均值的时候，这个效率比groupByKey的要高，它会在本地先合并数据，然后在发送数据，这样减少发送的数据
     * 减少带宽和内存的占用
     */
    val rdd = sc.parallelize(arr).map((_,1)).reduceByKey(_+_)

    rdd.foreach(println)

    sc.stop()
  }

}
