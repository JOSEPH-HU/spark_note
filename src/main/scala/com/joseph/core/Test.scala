package com.joseph.core

object Test {
  def main(args: Array[String]): Unit = {
    var a = 0;
    val numList = List(1,2,3,4,5,6)
    val numList1 = List(1,2,3);

    // for 循环
    for( a <- numList;b<- numList1){
      println( "Value of a: " + a + "-" + b )
    }
  }
}
