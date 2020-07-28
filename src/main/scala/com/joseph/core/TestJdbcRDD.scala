package com.joseph.core

import java.sql.{Connection, DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object TestJdbcRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestJdbcRDD").setMaster("local[1]")
    val sc = new SparkContext(conf)

    def getConnection() = {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "root")
    }

    def flatValue(result: ResultSet) = {
      (result.getInt("id"), result.getInt("age"))
    }

    val rdd = new JdbcRDD(
      sc,
      getConnection, //获得链接
      "select id,age from t_1 where id >=? and id <= ?",
      1,//id的最小值范围
      21,//id最大值的范围
      5,//两个分区 执行的过程 分区1(1,10) 分区2(11,21)一次执行两个查询，分区就相当于并行度,如果id相同，并且条数多，也是一个分区
      flatValue//查出的数据进行处理
    )

    rdd.foreach(println)

    sc.stop()
  }

}
