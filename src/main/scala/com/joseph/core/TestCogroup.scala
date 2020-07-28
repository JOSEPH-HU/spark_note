package com.joseph.core

import org.apache.spark.{SparkConf, SparkContext}

object TestCogroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestCogroup").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val arr1 = List(("a",1),("a",2),("a",3),("b",2),("b",1),("c",4),("c",5),("a",9))

    val arr2 = List(("a",1),("b",2),("d",4))

    val rdd1 = sc.parallelize(arr1)
    val rdd2 = sc.parallelize(arr2)

    val result = rdd1.cogroup(rdd2).foreach(line=>{
      val key = line._1
      val r1:List[Int] = line._2._1.toList
      for(a<-r1){
        println("rdd1" + key + "=" + a)
      }

      val r2:List[Int] = line._2._2.toList
      for(a<-r2){
        println("rdd2" + key + "=" + a)
      }
    })


    val result1 = rdd1.cogroup(rdd2).flatMapValues(pair =>

      for (v <- pair._1.iterator; w <- pair._2.iterator) yield (v, w)

    )
      .foreach(println)

    sc.stop()
  }

}
