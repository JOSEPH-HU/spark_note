package com.joseph.core

import org.apache.spark.{SparkConf, SparkContext}

object TestCoalesce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestCoalesce").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val arr1 = List(("a",1),("a",2),("a",3),("b",2),("b",1),("c",4),("c",5),("a",9),("e",4),("f",4),("g",4),("g",4))

    val rdd = sc.parallelize(arr1,3)

    /**
     * 返回一个经过简化到numPartitions个分区的新RDD。这会导致一个窄依赖，例如：你将1000个分区转换成100个分区，
     * 这个过程不会发生shuffle，相反如果10个分区转换成100个分区将会发生shuffle。然而如果你想大幅度合并分区，
     * 例如合并成一个分区，这会导致你的计算在少数几个集群节点上计算（言外之意：并行度不够）。为了避免这种情况，
     * 你可以将第二个shuffle参数传递一个true，这样会在重新分区过程中多一步shuffle，这意味着上游的分区可以并行运行。
     * 注意：第二个参数shuffle=true，将会产生多于之前的分区数目，例如你有一个个数较少的分区，假如是100，调用coalesce(1000,
     * shuffle = true)将会使用一个  HashPartitioner产生1000个分区分布在集群节点上。这个（对于提高并行度）是非常有用的。
     *
     */
    val rdd1 = rdd.coalesce(7,false)

    rdd1.foreach(println)

    sc.stop()
  }

}
