package org.daselab.sparkel.unused

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import main.scala.org.daselab.sparkel.Constants._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

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
//      sc.setCheckpointDir(CheckPointDir) //set checkpoint directory. See directions here: https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-rdd-checkpointing.html
      
      //var(uAxioms,type1Axioms) = initializeRDD(sc, args(0)) 
      
      var uAxioms = sc.textFile(args(0)+"sAxioms.txt").map(line => {line.split("\\|") match { case Array(x,y) => (y.toInt,x.toInt)}})     
  //    uAxioms = uAxioms.cache()
      println("Before iteration uAxioms count: "+uAxioms.count())
      //TODO? call parallelize on uAxioms?
      
      var type1Axioms = sc.textFile(args(0)+"Type1Axioms.txt").map(line => {line.split("\\|") match { case Array(x,y) => (x.toInt,y.toInt)}}) 
      type1Axioms = type1Axioms.cache()
      val count = type1Axioms.count()
      //TODO? call parallelize on type1Axioms?
      
       var counter=0;
      
      /*
      println("=======================Running non-iteratie version================")
      
     
      var t_beginLoop = System.nanoTime()
      val r1Join = type1Axioms.join(uAxioms).map( { case (k,v) => v}).cache() 
      val uAxioms1 = uAxioms.union(r1Join).distinct.cache()
      
      println(uAxioms1.toDebugString)
      println("uAxioms count: "+uAxioms1.count())     
      
        var t_endLoop = System.nanoTime()
        println("End of loop "+counter+": Time for this loop: "+(t_endLoop - t_beginLoop)/1e6 +" ms")        
        println("=======================================================================================")
        
        counter +=1
        
      t_beginLoop = System.nanoTime()  
      val r2Join1 = type1Axioms.join(uAxioms1).map( { case (k,v) => v}).cache()
      val uAxioms2 = uAxioms1.union(r2Join1).distinct.cache()
             
      println(uAxioms2.toDebugString)
            println("uAxioms count: "+uAxioms2.count())     
      
        t_endLoop = System.nanoTime()
        println("End of loop "+counter+": Time for this loop: "+(t_endLoop - t_beginLoop)/1e6 +" ms")        
        println("=======================================================================================")

        counter +=1
        
      t_beginLoop = System.nanoTime()
      val r2Join2 = type1Axioms.join(uAxioms2).map( { case (k,v) => v}).cache()
      val uAxioms3 = uAxioms1.union(r2Join2).distinct.cache()

       println(uAxioms3.toDebugString)
            println("uAxioms count: "+uAxioms3.count())     
      
        t_endLoop = System.nanoTime()
        println("End of loop "+counter+": Time for this loop: "+(t_endLoop - t_beginLoop)/1e6 +" ms")        
        println("=======================================================================================")

     counter +=1
        
        t_beginLoop = System.nanoTime()  
      val r2Join3 = type1Axioms.join(uAxioms3).map( { case (k,v) => v}).cache()
      val uAxioms4 = uAxioms1.union(r2Join3).distinct.cache()
      
       println(uAxioms4.toDebugString)
            println("uAxioms count: "+uAxioms4.count())     
      
        t_endLoop = System.nanoTime()
        println("End of loop "+counter+": Time for this loop: "+(t_endLoop - t_beginLoop)/1e6 +" ms")        
        println("=======================================================================================")

        counter +=1
        
      t_beginLoop = System.nanoTime()
      val r2Join4 = type1Axioms.join(uAxioms4).map( { case (k,v) => v}).cache()
      val uAxioms5 = uAxioms1.union(r2Join4).distinct.cache()
 
      println(uAxioms5.toDebugString)
            println("uAxioms count: "+uAxioms5.count())     
      
        t_endLoop = System.nanoTime()
        println("End of loop "+counter+": Time for this loop: "+(t_endLoop - t_beginLoop)/1e6 +" ms")        
        println("=======================================================================================")

        counter +=1
        
      t_beginLoop = System.nanoTime()
      val r2Join5 = type1Axioms.join(uAxioms5).map( { case (k,v) => v}).cache()
      val uAxioms6 = uAxioms1.union(r2Join5).distinct.cache()

       println(uAxioms6.toDebugString)
            println("uAxioms count: "+uAxioms6.count())     
      
        t_endLoop = System.nanoTime()
        println("End of loop "+counter+": Time for this loop: "+(t_endLoop - t_beginLoop)/1e6 +" ms")        
        println("=======================================================================================")

        counter +=1
        
      t_beginLoop = System.nanoTime()
      val r2Join6 = type1Axioms.join(uAxioms6).map( { case (k,v) => v}).cache()
      val uAxioms7 = uAxioms1.union(r2Join6).distinct.cache()

       println(uAxioms7.toDebugString)
            println("uAxioms count: "+uAxioms7.count())     
      
        t_endLoop = System.nanoTime()
        println("End of loop "+counter+": Time for this loop: "+(t_endLoop - t_beginLoop)/1e6 +" ms")        
        println("=======================================================================================")

        counter +=1
        
              t_beginLoop = System.nanoTime()
      val r2Join7 = type1Axioms.join(uAxioms7).map( { case (k,v) => v}).cache()
      val uAxioms8 = uAxioms1.union(r2Join7).distinct.cache()
         println(uAxioms8.toDebugString)
            println("uAxioms count: "+uAxioms8.count())     
      
        t_endLoop = System.nanoTime()
        println("End of loop "+counter+": Time for this loop: "+(t_endLoop - t_beginLoop)/1e6 +" ms")        
        println("=======================================================================================")

        counter +=1
        
              t_beginLoop = System.nanoTime()
      val r2Join8 = type1Axioms.join(uAxioms8).map( { case (k,v) => v}).cache()
      val uAxioms9 = uAxioms1.union(r2Join8).distinct.cache()
         println(uAxioms9.toDebugString)
            println("uAxioms count: "+uAxioms9.count())     
      
        t_endLoop = System.nanoTime()
        println("End of loop "+counter+": Time for this loop: "+(t_endLoop - t_beginLoop)/1e6 +" ms")        
        println("=======================================================================================")

        counter +=1
        
              t_beginLoop = System.nanoTime()
      val r2Join9 = type1Axioms.join(uAxioms9).map( { case (k,v) => v}).cache()
      val uAxioms10 = uAxioms1.union(r2Join9).distinct.cache()
             println(uAxioms10.toDebugString)
            println("uAxioms count: "+uAxioms10.count())     
      
        t_endLoop = System.nanoTime()
        println("End of loop "+counter+": Time for this loop: "+(t_endLoop - t_beginLoop)/1e6 +" ms")        
        println("=======================================================================================")

      */
       
      
        //iterative version
       println("==============================Running iterative version==============================")
       
       //var uAxiomsNew = uAxioms
        
       while(counter < 1000){
       
              
        val t_beginLoop = System.nanoTime()
        val type1AxiomsL = type1Axioms
        val r1Join = type1AxiomsL.join(uAxioms).map( { case (k,v) => v})
        var count: Long = r1Join.count()
        println("r1Join count: " + count)
        var uAxiomsL = uAxioms.union(r1Join)
        
        println("uAxioms count before distinct: "+uAxiomsL.count())
        uAxiomsL = uAxiomsL.distinct
//        uAxioms = null
         
        uAxioms = uAxiomsL.repartition(2).cache()
        count = uAxioms.count()
        //forget old uAxioms
        //uAxioms.unpersist()
        
        //uAxioms = uAxiomsNew
//        uAxioms = uAxioms.repartition(4).cache()
        
        //testing checkpoint
        //if(counter == 4)
        //uAxioms.checkpoint()
        
        //println(uAxioms.toDebugString)
        
        
        //debugging 
        counter += 1
        
        val t_endLoop = System.nanoTime()
        //uAxioms.foreach(println(_))
        
        //debugging
        println("End of loop "+counter+": Time for this loop: "+(t_endLoop - t_beginLoop)/1e6 +" ms"+" uAxioms.isCheckpointed: "+ uAxioms.isCheckpointed)        
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