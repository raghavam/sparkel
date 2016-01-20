package org.daselab.sparkel

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.rdd._
import java.io.File
import org.apache.spark.storage.StorageLevel
import main.scala.org.daselab.sparkel.Constants._

/**
 * Distributed EL reasoning using Spark
 * This object has functions corresponding to each EL completion rule. 
 * 
 * @author Sambhawa Priya
 */
object DebugSpark {
  
   /*
   * Initializes all the RDDs corresponding to each axiom-type. 
   */
  def initializeRDD(sc: SparkContext, dirPath: String) = {    
    
    //!!!Remember to SWAP x and y here for testing with real ontologies. Keep as is for testing with sample test files.
    var uAxioms = sc.textFile(dirPath+"sAxioms.txt").map(line => {line.split("\\|") match { case Array(x,y) => (y.toInt,x.toInt)}}) 
    //checkpoint uAxioms
    uAxioms.checkpoint()
    uAxioms.count()//force action
    println("Inside initializeRDD, uAxioms.isCheckpointed: "+uAxioms.isCheckpointed)
    
    //rAxioms initialized only for testing individual rules 4,5, and 6.
    //var rAxioms = sc.textFile(dirPath+"rAxioms.txt").map(line => {line.split("\\|") match { case Array(x,y,z) => (x.toInt,(y.toInt,z.toInt))}})
    //rAxioms must be empty intially for final algorithm (use above initialization of rAxiom for testing purposes)
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
    
    //return the initialized RDDs as a Tuple object (can at max have 22 elements in Spark Tuple)
     (uAxioms,rAxioms, type1Axioms,type2Axioms,type3Axioms,type4Axioms,type5Axioms,type6Axioms)   
  }
  
  //completion rule1
  def completionRule1(uAxioms: RDD[(Int,Int)], type1Axioms: RDD[(Int,Int)]): RDD[(Int,Int)] = {
    
    val r1Join = type1Axioms.join(uAxioms).map( { case (k,v) => v})
    val uAxiomsNew = uAxioms.union(r1Join).distinct // uAxioms is immutable as it is input parameter
    
    //debugging
   // println("Rule1- new uAxioms count: "+(uAxiomsNew.count-uAxioms.count))
   
    //checkpointing successful for uAxioms!!
//    uAxioms.checkpoint()
//    uAxioms.count() // force action
//    println("uAxioms.isCheckpointed: "+uAxioms.isCheckpointed)
   
    //checkpointing successful for uAxiomsNew also!!
//    uAxiomsNew.checkpoint()
//    uAxiomsNew.count()
//    println("uAxiomsNew.isCheckpointed: "+uAxiomsNew.isCheckpointed)
    
    uAxiomsNew    
  }
  
  //completion rule 2
  def completionRule2(uAxioms: RDD[(Int,Int)], type2Axioms: RDD[(Int,(Int,Int))]): RDD[(Int,Int)] = {
  
    val r2Join1 = type2Axioms.join(uAxioms)
    val r2Join1Remapped = r2Join1.map({ case (k,((v1,v2),v3)) => (v1,(v2,v3))})
    val r2Join2 = r2Join1Remapped.join(uAxioms)
    val r2JoinOutput = r2Join2.filter({case (k,((v1,v2),v3)) => v2 == v3}).map( {case (k,((v1,v2),v3)) => (v1,v2)})
    val uAxiomsNew = uAxioms.union(r2JoinOutput).distinct // uAxioms is immutable as it is input parameter
    
    //debugging
    println("Rule2- new uAxioms count: "+(uAxiomsNew.count-uAxioms.count))
   
    
    uAxiomsNew 
    
  }
  
  //completion rule 3
  def completionRule3(uAxioms: RDD[(Int,Int)], rAxioms: RDD[(Int,(Int,Int))], type3Axioms: RDD[(Int,(Int,Int))]): RDD[(Int,(Int,Int))] = {
    
    val r3Join = type3Axioms.join(uAxioms)
    val r3Output = r3Join.map({case (k,((v1,v2),v3)) => (v1,(v3,v2))})
    val rAxiomsNew = rAxioms.union(r3Output).distinct
    
    //debugging
    println("Rule3- new rAxioms count: "+(rAxiomsNew.count-rAxioms.count))
    
    
    rAxiomsNew
    
  }
  
  //completion rule 4
  def completionRule4(uAxioms: RDD[(Int,Int)], rAxioms: RDD[(Int,(Int,Int))], type4Axioms: RDD[(Int,(Int,Int))]): RDD[(Int,Int)] = {
  
    val r4Join1 = type4Axioms.join(rAxioms).map({case (k,((v1,v2),(v3,v4))) => (v1,(v2,(v3,v4)))})
    val r4Join2 = r4Join1.join(uAxioms).filter({case (k,((v2,(v3,v4)),v5)) => v4 == v5 }).map({case (k,((v2,(v3,v4)),v5)) => (v2,v3)})
    val uAxiomsNew = uAxioms.union(r4Join2).distinct
    
    //debugging
    println("Rule4 - new uAxioms count: "+ (uAxiomsNew.count-uAxioms.count))
    
    
    
    uAxiomsNew   
  }
  
  //completion rule 5
   def completionRule5(rAxioms: RDD[(Int,(Int,Int))], type5Axioms: RDD[(Int,Int)]): RDD[(Int,(Int,Int))] = {
    
     val r5Join = type5Axioms.join(rAxioms).map({case (k,(v1,(v2,v3))) => (v1,(v2,v3))})
     val rAxiomsNew = rAxioms.union(r5Join).distinct
     
     //debugging
     println("Rule5 - new rAxioms count: "+(rAxiomsNew.count-rAxioms.count))
   
    
     rAxiomsNew
   }
   
   //completion rule 6
   def completionRule6(rAxioms: RDD[(Int,(Int,Int))], type6Axioms: RDD[(Int,(Int,Int))]): RDD[(Int,(Int,Int))] = {
    
     val r6Join1 = type6Axioms.join(rAxioms).map({case (k,((v1,v2),(v3,v4))) => (v1,(v2,(v3,v4)))})
     val r6Join2 = r6Join1.join(rAxioms).filter({case (k,((v2,(v3,v4)),(v5,v6))) => v4 == v5}).map({case (k,((v2,(v3,v4)),(v5,v6))) => (v2,(v3,v6))})
     val rAxiomsNew = rAxioms.union(r6Join2).distinct
     
     //debugging
     println("Rule6- new rAxioms count: "+(rAxiomsNew.count-rAxioms.count))
       
     rAxiomsNew
   }
   
   //Computes time of any function passed to it
   def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1e6 + " ms")
    result
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
      
      val t_init = System.nanoTime()
      
      val conf = new SparkConf().setAppName("SparkEL")
      val sc = new SparkContext(conf)
      sc.setCheckpointDir(CheckPointDir) //set checkpoint directory. See directions here: https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-rdd-checkpointing.html
      
      val(uAxioms,rAxioms, type1Axioms,type2Axioms,type3Axioms,type4Axioms,type5Axioms,type6Axioms) = initializeRDD(sc, args(0))
     
      //compute closure
      var prevUAxiomsCount: Long = 0
      var prevRAxiomsCount: Long = 0       
      var currUAxiomsCount: Long = uAxioms.count
      var currRAxiomsCount: Long = rAxioms.count
      
      println("Before closure computation. Initial #uAxioms: "+ currUAxiomsCount+", Initial #rAxioms: "+ currRAxiomsCount)
      var counter=0;
      var uAxiomsFinal=uAxioms
      while(counter < 10){
       
        val t_beginLoop = System.nanoTime()
        
        //debugging 
        counter=counter+1
 
        val uAxiomsRule1 = completionRule1(uAxiomsFinal, type1Axioms) //Rule1
       
        //debug
        println("uAxiomsRule1 dependencies:\n "+ uAxiomsRule1.toDebugString)
        
        //checkpoint
//         uAxiomsRule1.checkpoint()
//         uAxiomsRule1.count() // force action
//         println("--------------checkpoint info---------------")
//         println("uAxiomsRule1.isCheckpointed inside loop: "+uAxiomsRule1.isCheckpointed)
           
         uAxiomsFinal=uAxiomsRule1
        
        val t_endLoop = System.nanoTime()
        
        //debugging
        println("===================================debug info=========================================")
        println("End of loop: "+counter)        
        println("Time for this loop: "+(t_endLoop - t_beginLoop)/1e6 +" ms")
        println("=======================================================================================")
      }
      
      val t_end = System.nanoTime()
      
      println("Closure computed in "+(t_end - t_init)/1e6+" ms. Final number of uAxioms: "+ uAxiomsFinal.count)
      uAxiomsFinal.foreach(println(_))
      
      //testing individual rules
//      println("Before: uAxioms count is "+ uAxioms.distinct.count+" and rAxioms count is: "+rAxioms.count); //uAxioms.distinct ensures we don't account for dups
//      //uAxioms = completionRule2(uAxioms,type2Axioms);
//      //uAxioms = completionRule4(uAxioms,rAxioms,type4Axioms);
//      rAxioms = completionRule6(rAxioms,type6Axioms)
//      println("After: uAxioms count is- "+ uAxioms.count+" and rAxioms count is: "+rAxioms.count);
      
    }
  }
  
  
  
  
}