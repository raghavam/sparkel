package org.daselab.sparkel

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import main.scala.org.daselab.sparkel.Constants._

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
    val type2Axioms = sc.textFile(dirPath+"Type2Axioms.txt").map(line => {line.split("\\|") match { case Array(x,y,z) => (x.toInt,(y.toInt,z.toInt))}})
    
    //persist the RDDs
    type1Axioms.persist()
    type2Axioms.persist()
    
    //return the uAxioms and 
     (uAxioms,type1Axioms,type2Axioms)   
  }
  
  //completion rule1
  def completionRule1(uAxioms: RDD[(Int,Int)], type1Axioms: RDD[(Int,Int)]) = {
    val r1Join = type1Axioms.join(uAxioms).map( { case (k,v) => v})
    val uAxiomsNew = uAxioms.union(r1Join).distinct // uAxioms is immutable as it is input parameter
    uAxiomsNew    
  }
  
  //completion rule 2
  def completionRule2(uAxioms: RDD[(Int,Int)], type2Axioms: RDD[(Int,(Int,Int))]) = {
  
    val r2Join1 = type2Axioms.join(uAxioms)
    val r2Join1Remapped = r2Join1.map(pair => pair._2).map( pairNew => (pairNew._1._1,(pairNew._1._2,pairNew._2)))
    val r2Join2 = r2Join1Remapped.join(uAxioms)
    val r2JoinOutput = r2Join2.filter({case (a,((b,c),d)) => c == d}).map(pair => pair._2._1)
    val uAxiomsNew = uAxioms.union(r2JoinOutput).distinct // uAxioms is immutable as it is input parameter
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
      var(uAxioms,type1Axioms,type2Axioms) = initializeRDD(sc, args(0))
      println("Before: uAxioms count is- "+ uAxioms.count);
      uAxioms = completionRule1(uAxioms,type1Axioms);
      println("After: uAxioms count is- "+ uAxioms.count);
      
    }
  }
  
  
}