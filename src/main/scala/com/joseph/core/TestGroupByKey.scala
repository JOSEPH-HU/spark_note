package com.joseph.core

import org.apache.spark.{SparkConf, SparkContext}

object TestGroupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestGroupByKey").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val arr = Array(1,1,1,2,3,4,4,5,5,6,6,6,6)
    /*×
    *1.如果求和和计算平均值，最好用reduceByKey和aggregateByKey代替，这样可以提高性能，groupByKey把key相同的数据拉取同一个节点
    * 在计算，这样拉取的数据量非常大，其他两个在拉取的时候，先合并数据，这样减少数据的两，节省带宽和io
    * 2.如果不传的分区参数时指是父RDD的分区数，为了增加任务的并发度，可以加大分区数
     */
    val rdd = sc.parallelize(arr,3).map(line=>(line,1)).groupByKey(2)
        .map(line=>{
          var tmp = line._2.foldLeft(0)(_+_)
          (line._1,tmp)

        })
    rdd.foreach(println)
  }

}
