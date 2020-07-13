package com.joseph.core

import org.apache.spark.{SparkConf, SparkContext}

object TestSample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TestSample").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val arr = Array(1,2,3,4,5,6,7,8,9,10)
    //元素不可以多次抽样,withReplacement=false，每个元素被抽取到的概率为0.5：fraction=0.5  fraction必须在0到1之间
    //元素可以多次抽样：withReplacement=true，每个元素被抽取到的期望次数为2：fraction=2 fraction必须大于1
    //第三个参数seed默认就可以
    val rdd = sc.parallelize(arr).sample(true,2)

    rdd.foreach(println)

  }

}
