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
     
    val type1Axioms = sc.textFile(dirPath+"Type1Axioms.txt").map(line => {line.split("\\|") match { case Array(x,y) => (x.toInt,y.toInt)}}) 
    
    //return the initialized RDDs as a Tuple object (can at max have 22 elements in Spark Tuple)
     (uAxioms,type1Axioms)   
  }
  
  //completion rule1
  def completionRule1(uAxioms: RDD[(Int,Int)], type1Axioms: RDD[(Int,Int)]): RDD[(Int,Int)] = {
    
    val r1Join = type1Axioms.join(uAxioms).map( { case (k,v) => v})
    //r1Join.count()
    //val r1JoinMapped = r1Join.map( { case (k,v) => v}).cache()
    //r1JoinMapped.count()
    
    val uAxiomsNew = uAxioms.union(r1Join).distinct // uAxioms is immutable as it is input parameter    
    //uAxiomsNew.cache()
    //uAxiomsNew.count()
   
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
      
      val t_init = System.nanoTime()
      
      val conf = new SparkConf().setAppName("DebugSpark")
      val sc = new SparkContext(conf)
     // sc.setCheckpointDir(CheckPointDir) //set checkpoint directory. See directions here: https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-rdd-checkpointing.html
      
      var(uAxioms,type1Axioms) = initializeRDD(sc, args(0))      
      uAxioms.cache()
      uAxioms.count()
      
      type1Axioms.cache()
      type1Axioms.count()
      
      //compute closure
       var currUAxiomsCount: Long = uAxioms.count
      
      
      println("Before closure computation. Initial #uAxioms: "+ currUAxiomsCount)
      var counter=0;
      //var uAxiomsFinal=uAxioms
      
      while(counter < 10){
       
              
        val t_beginLoop = System.nanoTime()
        
        uAxioms = completionRule1(uAxioms, type1Axioms) //Rule1
        uAxioms.cache()        
        println("uAxioms count: "+uAxioms.count())
        
        //debugging 
        counter += 1
        
        val t_endLoop = System.nanoTime()
        
        //debugging
        println("End of loop "+counter+": Time for this loop: "+(t_endLoop - t_beginLoop)/1e6 +" ms")        
        println("=======================================================================================")
      }
      
      val t_end = System.nanoTime()
      
      println("Closure computed in "+(t_end - t_init)/1e6+" ms. Final number of uAxioms: "+ uAxioms.count)
      //uAxioms.foreach(println(_))
      
      //testing individual rules
//      println("Before: uAxioms count is "+ uAxioms.distinct.count+" and rAxioms count is: "+rAxioms.count); //uAxioms.distinct ensures we don't account for dups
//      //uAxioms = completionRule2(uAxioms,type2Axioms);
//      //uAxioms = completionRule4(uAxioms,rAxioms,type4Axioms);
//      rAxioms = completionRule6(rAxioms,type6Axioms)
//      println("After: uAxioms count is- "+ uAxioms.count+" and rAxioms count is: "+rAxioms.count);
      
    }
  }
  
  
  
  
}