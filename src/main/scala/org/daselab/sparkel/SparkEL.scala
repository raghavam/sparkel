package org.daselab.sparkel

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.EmptyRDD
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
    //!!!SWAP x and y here
    var uAxioms = sc.textFile(dirPath+"uAxioms.txt").map(line => {line.split("\\|") match { case Array(x,y) => (x.toInt,y.toInt)}}) 
    var rAxioms: RDD[(Int,(Int,Int))] = sc.emptyRDD
      
    val type1Axioms = sc.textFile(dirPath+"Type1Axioms.txt").map(line => {line.split("\\|") match { case Array(x,y) => (x.toInt,y.toInt)}}) 
    val type2Axioms = sc.textFile(dirPath+"Type2Axioms.txt").map(line => {line.split("\\|") match { case Array(x,y,z) => (x.toInt,(y.toInt,z.toInt))}})
    val type3Axioms = sc.textFile(dirPath+"Type3Axioms.txt").map(line => {line.split("\\|") match { case Array(x,y,z) => (x.toInt,(y.toInt,z.toInt))}})
    val type4Axioms = sc.textFile(dirPath+"Type4Axioms.txt").map(line => {line.split("\\|") match { case Array(x,y,z) => (x.toInt,(y.toInt,z.toInt))}})
    val type5Axioms = sc.textFile(dirPath+"Type5Axioms.txt").map(line => {line.split("\\|") match { case Array(x,y) => (x.toInt,y.toInt)}})
    val type6Axioms = sc.textFile(dirPath+"Type6Axioms.txt").map(line => {line.split("\\|") match { case Array(x,y,z) => (x.toInt,(y.toInt,z.toInt))}})
    
    
    //persist the RDDs
    type1Axioms.persist()
    type2Axioms.persist()
    type3Axioms.persist()
    type4Axioms.persist()
    type5Axioms.persist()
    type6Axioms.persist()
    
    //return the uAxioms and 
     (uAxioms,rAxioms, type1Axioms,type2Axioms,type3Axioms,type4Axioms,type5Axioms,type6Axioms)   
  }
  
  //completion rule1
  def completionRule1(uAxioms: RDD[(Int,Int)], type1Axioms: RDD[(Int,Int)]): RDD[(Int,Int)] = {
    val r1Join = type1Axioms.join(uAxioms).map( { case (k,v) => v})
    val uAxiomsNew = uAxioms.union(r1Join).distinct // uAxioms is immutable as it is input parameter
    uAxiomsNew    
  }
  
  //completion rule 2
  def completionRule2(uAxioms: RDD[(Int,Int)], type2Axioms: RDD[(Int,(Int,Int))]): RDD[(Int,Int)] = {
  
    val r2Join1 = type2Axioms.join(uAxioms)
    val r2Join1Remapped = r2Join1.map({ case (k,((v1,v2),v3)) => (v1,(v2,v3))})
    println("Join1+map output: ")
    r2Join1Remapped.foreach(println(_))
    val r2Join2 = r2Join1Remapped.join(uAxioms)
    println("Join2 output")
    r2Join2.foreach(println(_))
    val r2JoinOutput = r2Join2.filter({case (k,((v1,v2),v3)) => v2 == v3}).map( {case (k,((v1,v2),v3)) => (v1,v2)})
    println("Filter and map on Join2 output")
    r2JoinOutput.foreach(println(_))
    val uAxiomsNew = uAxioms.union(r2JoinOutput).distinct // uAxioms is immutable as it is input parameter
    println("Final output after filter")
    uAxiomsNew.foreach(println(_))
    uAxiomsNew 
    
  }
   
  def completionRule3(uAxioms: RDD[(Int,Int)], rAxioms: RDD[(Int,(Int,Int))], type3Axioms: RDD[(Int,(Int,Int))]): RDD[(Int,(Int,Int))] = {
    
    val r3Join = type3Axioms.join(uAxioms)
    val r3Output = r3Join.map({case (k,((v1,v2),v3)) => (v1,(v3,v2))})
    val rAxiomsNew = rAxioms.union(r3Output).distinct
    rAxiomsNew
    
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
      var(uAxioms,rAxioms, type1Axioms,type2Axioms,type3Axioms,type4Axioms,type5Axioms,type6Axioms) = initializeRDD(sc, args(0))
      println("Before: uAxioms count is "+ uAxioms.distinct.count+" and rAxioms count is: "+rAxioms.count); //uAxioms.distinct ensures we don't account for dups
      uAxioms = completionRule2(uAxioms,type2Axioms);
      //rAxioms = completionRule3(uAxioms,rAxioms,type3Axioms);
      println("After: uAxioms count is- "+ uAxioms.count+" and rAxioms count is: "+rAxioms.count);
      
    }
  }
  
  
}