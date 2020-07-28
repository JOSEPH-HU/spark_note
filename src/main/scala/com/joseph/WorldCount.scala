package com.joseph

import org.apache.spark.{SparkConf, SparkContext}

object WorldCount {
  def main(args: Array[String]): Unit = {
    val logfile = "/home/hlw/app/spark-3.0.0-preview2-bin-custom-spark/README.md"
    val conf = new SparkConf().setAppName("WorldCount").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val logData = sc.textFile(logfile)

    val result = logData.flatMap(line=>line.split(" ")).map(line=>(line,1)).reduceByKey(_+_)
    println(result.dependencies)
    result.foreach(line=>println(line._1 + "=" + line._2))

    sc.stop()

  }

}
