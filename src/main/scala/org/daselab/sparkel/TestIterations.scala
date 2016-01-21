package org.daselab.sparkel

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object TestIterations {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("TestIterations")
    val sc = new SparkContext(conf)
    var rdd = sc.parallelize(List(1, 2, 3, 4))
    var i = 0
    
    while(i < 3) {
      val t_beginLoop = System.nanoTime()
      
      val result = rdd.flatMap{x => Thread.sleep(1000); List(x)}
      rdd = result.map(x => x * 10)
      println("Count: " + rdd.count)
      i += 1
      
      val t_endLoop = System.nanoTime()
      println("Loop "+ i +": Time taken: "+ (t_endLoop - t_beginLoop)/1e6 +" ms.")
      
      
   }//while loop closes
    
    
    
  }
  
}