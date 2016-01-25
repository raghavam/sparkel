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
object SparkEL {
  
   /*
   * Initializes all the RDDs corresponding to each axiom-type. 
   */
  def initializeRDD(sc: SparkContext, dirPath: String) = {    
    
    //!!!Remember to SWAP x and y here for testing with real ontologies. Keep as is for testing with sample test files.
    var uAxioms = sc.textFile(dirPath+"sAxioms.txt").map(line => {line.split("\\|") match { case Array(x,y) => (y.toInt,x.toInt)}}) 
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
    type1Axioms.cache().count()
    type2Axioms.cache().count()
    type3Axioms.cache().count()
    type4Axioms.cache().count()
    type5Axioms.cache().count()
    type6Axioms.cache().count()
    
    //return the initialized RDDs as a Tuple object (can at max have 22 elements in Spark Tuple)
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
    val r2Join2 = r2Join1Remapped.join(uAxioms)
    val r2JoinOutput = r2Join2.filter({case (k,((v1,v2),v3)) => v2 == v3}).map( {case (k,((v1,v2),v3)) => (v1,v2)})
    val uAxiomsNew = uAxioms.union(r2JoinOutput).distinct // uAxioms is immutable as it is input parameter
   
    uAxiomsNew 
    
  }
  
  //completion rule 3
  def completionRule3(uAxioms: RDD[(Int,Int)], rAxioms: RDD[(Int,(Int,Int))], type3Axioms: RDD[(Int,(Int,Int))]): RDD[(Int,(Int,Int))] = {
    
    val r3Join = type3Axioms.join(uAxioms)
    val r3Output = r3Join.map({case (k,((v1,v2),v3)) => (v1,(v3,v2))})
    val rAxiomsNew = rAxioms.union(r3Output).distinct
    
    rAxiomsNew
    
  }
  
  //completion rule 4
  def completionRule4(uAxioms: RDD[(Int,Int)], rAxioms: RDD[(Int,(Int,Int))], type4Axioms: RDD[(Int,(Int,Int))]): RDD[(Int,Int)] = {
  
    val r4Join1 = type4Axioms.join(rAxioms).map({case (k,((v1,v2),(v3,v4))) => (v1,(v2,(v3,v4)))})
    val r4Join2 = r4Join1.join(uAxioms).filter({case (k,((v2,(v3,v4)),v5)) => v4 == v5 }).map({case (k,((v2,(v3,v4)),v5)) => (v2,v3)})
    val uAxiomsNew = uAxioms.union(r4Join2).distinct
    
    uAxiomsNew   
  }
  
  //completion rule 5
   def completionRule5(rAxioms: RDD[(Int,(Int,Int))], type5Axioms: RDD[(Int,Int)]): RDD[(Int,(Int,Int))] = {
    
     val r5Join = type5Axioms.join(rAxioms).map({case (k,(v1,(v2,v3))) => (v1,(v2,v3))})
     val rAxiomsNew = rAxioms.union(r5Join).distinct
    
     rAxiomsNew
   }
   
   //completion rule 6
   def completionRule6(rAxioms: RDD[(Int,(Int,Int))], type6Axioms: RDD[(Int,(Int,Int))]): RDD[(Int,(Int,Int))] = {
    
     val r6Join1 = type6Axioms.join(rAxioms).map({case (k,((v1,v2),(v3,v4))) => (v1,(v2,(v3,v4)))})
     val r6Join2 = r6Join1.join(rAxioms).filter({case (k,((v2,(v3,v4)),(v5,v6))) => v4 == v5}).map({case (k,((v2,(v3,v4)),(v5,v6))) => (v2,(v3,v6))})
     val rAxiomsNew = rAxioms.union(r6Join2).distinct
       
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
      
      //init time
      val t_init = System.nanoTime()
      
      val conf = new SparkConf().setAppName("SparkEL")
      val sc = new SparkContext(conf)
      sc.setCheckpointDir(CheckPointDir) //set checkpoint directory. See directions here: https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-rdd-checkpointing.html
      
      val(uAxioms,rAxioms, type1Axioms,type2Axioms,type3Axioms,type4Axioms,type5Axioms,type6Axioms) = initializeRDD(sc, args(0))
      uAxioms.cache()
      //println("Before iteration uAxioms count: "+uAxioms.count())
      
      //compute closure
      var prevUAxiomsCount: Long = 0
      var prevRAxiomsCount: Long = 0 
      var currUAxiomsCount: Long = uAxioms.count
      var currRAxiomsCount: Long = rAxioms.count
      
      println("Before closure computation. Initial uAxioms count: "+ currUAxiomsCount+", Initial rAxioms count: "+ currRAxiomsCount)
      var counter=0;
      var uAxiomsFinal=uAxioms
      var rAxiomsFinal=rAxioms
      
     // var rAxiomsRule5=rAxioms
     // var rAxiomsRule6=rAxioms
      
     while(prevUAxiomsCount != currUAxiomsCount || prevRAxiomsCount != currRAxiomsCount){
        
       var t_beginLoop = System.nanoTime()
       
        //debugging 
        counter=counter+1
        
//        if(counter > 1)
//        {
//          uAxioms = sc.objectFile[(Int,Int)](CheckPointDir+"uAxiom"+(counter-1))
//          rAxioms = sc.objectFile[(Int,(Int,Int))](CheckPointDir+"rAxiom"+(counter-1))
//          
//        }
        
        val uAxiomsRule1 = completionRule1(uAxiomsFinal, type1Axioms) //Rule1
        println("----Completed rule1----")
        
        //println("uAxiomsRule1 dependencies:\n"+uAxiomsRule1.toDebugString)
        //uAxiomsRule1.checkpoint()
        // uAxiomsRule1.count() // force action
        //println("uAxiomsRule1.isCheckpointed: "+uAxiomsRule1.isCheckpointed)
        
        
        val uAxiomsRule2 = completionRule2(uAxiomsRule1, type2Axioms) //Rule2
        println("----Completed rule2----")
//        println("uAxiomsRule2 dependencies:\n"+uAxiomsRule2.toDebugString)
//        uAxiomsRule2.checkpoint()
//        uAxiomsRule2.count() // force action
//        println("uAxiomsRule2.isCheckpointed: "+uAxiomsRule2.isCheckpointed)
        
        val rAxiomsRule3 = completionRule3(uAxiomsRule2, rAxiomsFinal, type3Axioms) //Rule3
        println("----Completed rule3----")
//        println("rAxiomsRule3 dependencies:\n"+rAxiomsRule3.toDebugString)
//        rAxiomsRule3.checkpoint()
//        rAxiomsRule3.count() // force action
//        println("rAxiomsRule3.isCheckpointed: "+rAxiomsRule3.isCheckpointed)
        
        val uAxiomsRule4 = completionRule4(uAxiomsRule2, rAxiomsRule3, type4Axioms) // Rule4
        println("----Completed rule4----")
//        println("uAxiomsRule4 dependencies:\n"+uAxiomsRule4.toDebugString)
//        uAxiomsRule4.checkpoint()
//        uAxiomsRule4.count() // force action
//        println("uAxiomsRule4.isCheckpointed: "+uAxiomsRule4.isCheckpointed)
        
        //optimization: 
        //Skip rules 5 and 6 which can't be triggered if rAxioms are not updated in previous loop or to this point in current loop
            
        //debug 
        //println("prevRAxiomsCount: "+prevRAxiomsCount+", currentRAxiomCount: "+currRAxiomsCount+", rAxioms.count: "+rAxiomsRule3.count)
        
       // if(prevRAxiomsCount != currRAxiomsCount || rAxiomsRule3.count > currRAxiomsCount){
              
        val rAxiomsRule5 = completionRule5(rAxiomsRule3, type5Axioms) //Rule5 
        println("----Completed rule5----")
//        println("rAxiomsRule5 dependencies:\n"+rAxiomsRule5.toDebugString)
//        rAxiomsRule5.checkpoint()
//        rAxiomsRule5.count() // force action
//        println("rAxiomsRule5.isCheckpointed: "+rAxiomsRule5.isCheckpointed)
          
        val rAxiomsRule6 = completionRule6(rAxiomsRule5, type6Axioms) //Rule6
        println("----Completed rule6----")
//        println("rAxiomsRule6 dependencies:\n"+rAxiomsRule6.toDebugString)
//        rAxiomsRule6.checkpoint()
//        rAxiomsRule6.count() // force action
//        println("rAxiomsRule6.isCheckpointed: "+rAxiomsRule6.isCheckpointed)
          
//        }
//        else {
//          println("Skipping Rules 5 and 6 since rAxiom was not updated in the previous loop or by Rule 3 in the current loop")
//        }
        
//        //debugging - add checkpointing to truncate lineage graph
//        uAxioms.persist()
//        rAxioms.persist()
//        uAxioms.checkpoint()
//        rAxioms.checkpoint()
        
        //debugging RDD lineage
        //cache -> mark for checkpoint -> count
       
//        uAxioms.cache
//        rAxioms.cache
//        
//        uAxioms.checkpoint
//        rAxioms.checkpoint
        
        uAxiomsFinal=uAxiomsRule4
        rAxiomsFinal=rAxiomsRule6
        
        uAxiomsFinal = uAxiomsFinal.repartition(2).cache()
        rAxiomsFinal = rAxiomsFinal.repartition(2)cache()
        
        //update counts
        prevUAxiomsCount = currUAxiomsCount
        prevRAxiomsCount = currRAxiomsCount
        
        //TODO?
        //Q1. should we checkpoint uAxiomsFinal and rAxiomsFinal?
        //Q2. should we ONLY checkpoint uAxiomsFinal and rAxiomsFinal to avoid overhead of reading each rule RDD from disk
//        if(counter == 3){
//        uAxiomsFinal.checkpoint()
//        rAxiomsFinal.checkpoint() 
//        }       
        
        currUAxiomsCount = uAxiomsFinal.count() 
        currRAxiomsCount = rAxiomsFinal.count() 
        
        //println("uAxiomsFinal.isCheckpointed inside loop: "+uAxiomsFinal.isCheckpointed)
        //println("rAxiomsFinal.isCheckpointed inside loop: "+rAxiomsFinal.isCheckpointed)
        
        //time
        var t_endLoop = System.nanoTime()
        
        
        //debugging
        println("===================================debug info=========================================")
        println("End of loop: "+counter+". uAxioms count: "+currUAxiomsCount+", rAxioms count: "+currRAxiomsCount)
        println("Runtime of the current loop: "+(t_endLoop - t_beginLoop)/1e6 + " ms")
        //println("uAxiomsFinal dependencies: "+ uAxiomsFinal.toDebugString)
        //println("rAxiomsFinal dependencies: "+ rAxiomsFinal.toDebugString)
        println("======================================================================================")
        
       
      }//end of loop
      
      println("Closure computed. Final number of uAxioms: "+ currUAxiomsCount)
      //uAxiomsFinal.foreach(println(_))
      for (sAxiom <- uAxiomsFinal) println(sAxiom._2+"|"+sAxiom._1)
      val t_end = System.nanoTime()
      
      println("Total runtime of the program: "+(t_end - t_init)/1e6+" ms")
      
      //testing individual rules
//      println("Before: uAxioms count is "+ uAxioms.distinct.count+" and rAxioms count is: "+rAxioms.count); //uAxioms.distinct ensures we don't account for dups
//      //uAxioms = completionRule2(uAxioms,type2Axioms);
//      //uAxioms = completionRule4(uAxioms,rAxioms,type4Axioms);
//      rAxioms = completionRule6(rAxioms,type6Axioms)
//      println("After: uAxioms count is- "+ uAxioms.count+" and rAxioms count is: "+rAxioms.count);
      
    }
  }
  
  
  
  
}