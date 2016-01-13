package org.daselab.sparkel

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

/**
 * Distributed EL reasoning using Spark
 * This object has functions corresponding to the algorithms for each EL completion rule. 
 * 
 * @author Sambhawa Priya
 */
object SparkEL {
  
   /*
   * Initializes all the RDDs corresponding to each axiom-type. 
   */
  def initializeRDD(sc: SparkContext, dirPath: String) = {    
    // question - what is the return type for uAxioms and type1Axioms 
    var uAxioms = sc.textFile(dirPath+"uAxioms.txt").map(line => {line.split("\\|") match { case Array(x,y) => (x.toInt,y.toInt)}}) 
    val type1Axioms = sc.textFile(dirPath+"Type1Axioms.txt").map(line => {line.split("\\|") match { case Array(x,y) => (x.toInt,y.toInt)}}) 
    
    //persist the RDDs
    //uAxioms.persist()
    type1Axioms.persist()
    
    //return the uAxioms and 
     (uAxioms,type1Axioms)   
  }
  
  def completionRule1(uAxioms: RDD[(Int,Int)], type1Axioms: RDD[(Int,Int)]) = {
    val r1Join = type1Axioms.join(uAxioms).map( pair => pair._2)
    val uAxiomsNew = uAxioms.union(r1Join).distinct // uAmioms is immutabe as it is input parameter
    uAxiomsNew    
  }
  
  /*
   * The main method that inititalizes and calls each function corresponding to the completion rule 
   */
  def main(args: Array[String]): Unit = {
    if(args.length != 1) {
      System.err.println("Missing path of directory containing the axiom files!")
      System.exit(-1)
    }
    else {   
      val conf = new SparkConf().setAppName("SparkEL")
      val sc = new SparkContext(conf)
      var(uAxioms,type1Axioms) = initializeRDD(sc, args(0))
      println("Before: uAxioms count is- "+ uAxioms.count);
      uAxioms = completionRule1(uAxioms,type1Axioms);
      println("After: uAxioms count is- "+ uAxioms.count);
      
    }
  }
  
  
}