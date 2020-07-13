package com.joseph.core

import org.apache.spark.{SparkConf, SparkContext}

object CreateRDD {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CreateRDD").setMaster("local")
    val sc = new SparkContext(conf)

    //val data = Array(1, 2, 3, 4, 5)
    //val distData = sc.parallelize(data)

    val distData = sc.textFile("/home/hlw/tmp/12.txt").
      flatMap(line=>line.split(" ")).map(line=>(line,1)).reduceByKey(_+_)

    distData.foreach(println)
  }

}
