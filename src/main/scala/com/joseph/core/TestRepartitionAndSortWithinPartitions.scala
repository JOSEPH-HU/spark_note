package com.joseph.core

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object TestRepartitionAndSortWithinPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestRepartitionAndSortWithinPartitions").setMaster("local[1]")
    val sc  = new SparkContext(conf)

    val arr1 = List(("a",1),("a",2),("a",3),("b",2),("b",1),("c",4),("c",5),("a",9),("e",4),("f",4),("g",4),("g",4))

    val rdd =sc.parallelize(arr1,2).repartitionAndSortWithinPartitions(new selfPartitioner)

    rdd.foreach(println)

    sc.stop()
  }

}

class selfPartitioner extends Partitioner{
  override def numPartitions: Int = 3

  override def getPartition(key: Any): Int = {
    key.hashCode()%numPartitions
  }
}
