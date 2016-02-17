package org.daselab.sparkel


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.rdd._
import java.io.File
import org.apache.spark.storage.StorageLevel
import main.scala.org.daselab.sparkel.Constants._

object SparkELAlgoOpt{
  
  /*
   * Initializes all the RDDs corresponding to each axiom-type. 
   */
  def initializeRDD(sc: SparkContext, dirPath: String) = {

    //!!!Remember to SWAP x and y here for testing with real ontologies. Keep as is for testing with sample test files.
    var uAxioms = sc.textFile(dirPath + "sAxioms.txt").map(line => { line.split("\\|") match { case Array(x, y) => (y.toInt, x.toInt) } })
    //rAxioms initialized only for testing individual rules 4,5, and 6.
    //var rAxioms = sc.textFile(dirPath+"rAxioms.txt").map(line => {line.split("\\|") match { case Array(x,y,z) => (x.toInt,(y.toInt,z.toInt))}})
    //rAxioms must be empty intially for final algorithm (use above initialization of rAxiom for testing purposes)
    var rAxioms: RDD[(Int, (Int, Int))] = sc.emptyRDD

    val type1Axioms = sc.textFile(dirPath + "Type1Axioms.txt").map(line => { line.split("\\|") match { case Array(x, y) => (x.toInt, y.toInt) } })
    val type2Axioms = sc.textFile(dirPath + "Type2Axioms.txt").map(line => { line.split("\\|") match { case Array(x, y, z) => (x.toInt, (y.toInt, z.toInt)) } })
    val type3Axioms = sc.textFile(dirPath + "Type3Axioms.txt").map(line => { line.split("\\|") match { case Array(x, y, z) => (x.toInt, (y.toInt, z.toInt)) } })
    val type4Axioms = sc.textFile(dirPath + "Type4Axioms.txt").map(line => { line.split("\\|") match { case Array(x, y, z) => (x.toInt, (y.toInt, z.toInt)) } })
    val type5Axioms = sc.textFile(dirPath + "Type5Axioms.txt").map(line => { line.split("\\|") match { case Array(x, y) => (x.toInt, y.toInt) } })
    val type6Axioms = sc.textFile(dirPath + "Type6Axioms.txt").map(line => { line.split("\\|") match { case Array(x, y, z) => (x.toInt, (y.toInt, z.toInt)) } })

    //persist the RDDs
    type1Axioms.cache().count()
    type2Axioms.cache().count()
    type3Axioms.cache().count()
    type4Axioms.cache().count()
    type5Axioms.cache().count()
    type6Axioms.cache().count()

    //return the initialized RDDs as a Tuple object (can at max have 22 elements in Spark Tuple)
    (uAxioms, rAxioms, type1Axioms, type2Axioms, type3Axioms, type4Axioms, type5Axioms, type6Axioms)
  }

  //completion rule1
  def completionRule1(uAxioms: RDD[(Int, Int)], type1Axioms: RDD[(Int, Int)]): RDD[(Int, Int)] = {

    val r1Join = type1Axioms.join(uAxioms).map({ case (k, v) => v }).distinct
   // val uAxiomsNew = uAxioms.union(r1Join).distinct // uAxioms is immutable as it is input parameter

    r1Join
  }

  //completion rule 2
  def completionRule2(uAxioms: RDD[(Int, Int)], type2Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, Int)] = {

    val r2Join1 = type2Axioms.join(uAxioms)
    val r2Join1Remapped = r2Join1.map({ case (k, ((v1, v2), v3)) => (v1, (v2, v3)) })
    val r2Join2 = r2Join1Remapped.join(uAxioms)
    val r2JoinOutput = r2Join2.filter({ case (k, ((v1, v2), v3)) => v2 == v3 }).map({ case (k, ((v1, v2), v3)) => (v1, v2) }).distinct
    //val uAxiomsNew = uAxioms.union(r2JoinOutput).distinct // uAxioms is immutable as it is input parameter

    r2JoinOutput

  }

  //completion rule 3
  def completionRule3(uAxioms: RDD[(Int, Int)], type3Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {

   // var t_begin = System.nanoTime()
    val r3Join = type3Axioms.join(uAxioms)
   // r3Join.cache().count
   // var t_end = System.nanoTime()
   // println("type3Axioms.join(uAxioms): " + (t_end - t_begin) / 1e6 + " ms")
    
   // t_begin = System.nanoTime()
    val r3Output = r3Join.map({ case (k, ((v1, v2), v3)) => (v1, (v3, v2)) }).distinct
   // r3Output.cache().count
   // t_end = System.nanoTime()
   // println("r3Join.map(...): " + (t_end - t_begin) / 1e6 + " ms")
    
   // t_begin = System.nanoTime()
    //val rAxiomsNew = rAxioms.union(r3Output).distinct()
   // rAxioms.cache().count
   // t_end = System.nanoTime()
   // println("rAxioms.union(r3Output).distinct(): " + (t_end - t_begin) / 1e6 + " ms")
    
    r3Output

  }

  //completion rule 4
  def completionRule4(uAxioms: RDD[(Int, Int)], rAxioms: RDD[(Int, (Int, Int))], type4Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, Int)] = {

    println("Debugging with persist(StorageLevel.MEMORY_ONLY_SER)")
    
    var t_begin = System.nanoTime()
    val r4Join1 = type4Axioms.join(rAxioms)
    val r4Join1_count = r4Join1.persist(StorageLevel.MEMORY_ONLY_SER).count
    var t_end = System.nanoTime()
    println("type4Axioms.join(rAxioms). Count= " +r4Join1_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    t_begin = System.nanoTime()
    val r4Join1ReMapped = r4Join1.map({ case (k, ((v1, v2), (v3, v4))) => (v1, (v2, (v3, v4))) })
    val r4Join1ReMapped_count = r4Join1ReMapped.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("r4Join1.map(...). Count = " +r4Join1ReMapped_count+", Time taken: "+ (t_end - t_begin) / 1e6 + " ms")
    
    t_begin = System.nanoTime()
    val r4Join2 = r4Join1ReMapped.join(uAxioms)
    val r4Join2_count = r4Join2.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("r4Join1ReMapped.join(uAxioms). Count= " + r4Join2_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
   
    t_begin = System.nanoTime()
    val r4Join2Filtered = r4Join2.filter({ case (k, ((v2, (v3, v4)), v5)) => v4 == v5 }).map({ case (k, ((v2, (v3, v4)), v5)) => (v2, v3) }).distinct
    val r4Join2Filtered_count = r4Join2Filtered.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("r4Join2.filter().map(). Count = " +r4Join2Filtered_count +", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
//    t_begin = System.nanoTime()
//    val uAxiomsNew = uAxioms.union(r4Join2Filtered).distinct
//    val uAxiomsNew_count = uAxiomsNew.cache().count
//    t_end = System.nanoTime()
//    println("uAxioms.union(r4Join2Filtered).distinct. Count=  " +uAxiomsNew_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    r4Join2Filtered
  }
  
   def completionRule4_new(uAxioms: RDD[(Int, Int)], rAxioms: RDD[(Int, (Int, Int))], type4Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, Int)] = {

    println("Debugging with persist(StorageLevel.MEMORY_ONLY_SER)")
    
    var t_begin = System.nanoTime()
    val r4Join1 = type4Axioms.join(rAxioms)
    val r4Join1_count = r4Join1.persist(StorageLevel.MEMORY_ONLY_SER).count
    var t_end = System.nanoTime()
    println("type4Axioms.join(rAxioms). Count= " +r4Join1_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    t_begin = System.nanoTime()
    val r4Join1ReMapped = r4Join1.map({ case (k, ((v1, v2), (v3, v4))) => (v4, (v2, (v3, v1))) })
    val r4Join1ReMapped_count = r4Join1ReMapped.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("r4Join1.map(...). Count = " +r4Join1ReMapped_count+", Time taken: "+ (t_end - t_begin) / 1e6 + " ms")
    
    val uAxiomsFlipped = uAxioms.map({ case (k1, v5) => (v5, k1) })
    
    t_begin = System.nanoTime()
    val r4Join2 = r4Join1ReMapped.join(uAxiomsFlipped)
    val r4Join2_count = r4Join2.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("r4Join1ReMapped.join(uAxioms). Count= " + r4Join2_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
   
    t_begin = System.nanoTime()
    val r4Join2Filtered = r4Join2.filter({ case (k, ((v2, (v3, v1)), k1)) => v1 == k1 }).map({ case (k, ((v2, (v3, v1)), k1)) => (v2, v3) }).distinct
    val r4Join2Filtered_count = r4Join2Filtered.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("r4Join2.filter().map(). Count = " +r4Join2Filtered_count +", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
//    t_begin = System.nanoTime()
//    val uAxiomsNew = uAxioms.union(r4Join2Filtered).distinct
//    val uAxiomsNew_count = uAxiomsNew.cache().count
//    t_end = System.nanoTime()
//    println("uAxioms.union(r4Join2Filtered).distinct. Count=  " +uAxiomsNew_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    r4Join2Filtered
  }

  //completion rule 5
  def completionRule5(rAxioms: RDD[(Int, (Int, Int))], type5Axioms: RDD[(Int, Int)]): RDD[(Int, (Int, Int))] = {

    val r5Join = type5Axioms.join(rAxioms).map({ case (k, (v1, (v2, v3))) => (v1, (v2, v3)) }).distinct
   // val rAxiomsNew = rAxioms.union(r5Join).distinct

    r5Join
  }

  //completion rule 6
  def completionRule6(rAxioms: RDD[(Int, (Int, Int))], type6Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {

    val r6Join1 = type6Axioms.join(rAxioms).map({ case (k, ((v1, v2), (v3, v4))) => (v1, (v2, (v3, v4))) })
    val r6Join2 = r6Join1.join(rAxioms).filter({ case (k, ((v2, (v3, v4)), (v5, v6))) => v4 == v5 }).map({ case (k, ((v2, (v3, v4)), (v5, v6))) => (v2, (v3, v6)) }).distinct
   // val rAxiomsNew = rAxioms.union(r6Join2).distinct

    r6Join2
  }

  //Computes time of any function passed to it
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1e6 + " ms")
    result
  }

  /*
   * The main method that inititalizes and calls each function corresponding to the completion rule 
   */
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Missing args: 1. path of directory containing the axiom files, 2. output directory to save the computed sAxioms")
      System.exit(-1)
    }

    //init time
    val t_init = System.nanoTime()

    val conf = new SparkConf().setAppName("SparkEL")
    val sc = new SparkContext(conf)
    
    val numProcessors = Runtime.getRuntime.availableProcessors()
    //      sc.setCheckpointDir(CheckPointDir) //set checkpoint directory. See directions here: https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-rdd-checkpointing.html

    var (uAxioms, rAxioms, type1Axioms, type2Axioms, type3Axioms, type4Axioms, type5Axioms, type6Axioms) = initializeRDD(sc, args(0))
    uAxioms = uAxioms.cache()
   
   
    println("Before closure computation. Initial uAxioms count: " + uAxioms.count + ", Initial rAxioms count: " + rAxioms.count)
    
    var uAxiomsFinal = uAxioms
    var rAxiomsFinal = rAxioms
    
    var currDeltaUAllRules = uAxioms
    var currDeltaRAllRules = rAxioms
    
    var currDeltaUAllRulesCount = uAxioms.count
    var currDeltaRAllRulesCount = rAxioms.count
    
    
    //1st iteration
     //println("Loop 1")
     
      var t_beginLoop = System.nanoTime()
     //Rule 1
      var currDeltaURule1 = completionRule1(currDeltaUAllRules, type1Axioms) //Rule1
      println("----Completed rule1----")
      
      
      //build input to rule 2
      var inputURule2 = sc.union(currDeltaUAllRules, currDeltaURule1)      
      var currDeltaURule2 = completionRule2(inputURule2, type2Axioms) //Rule2
      println("----Completed rule2----")


      var inputURule3 = sc.union(inputURule2,currDeltaURule2)
      var currDeltaRRule3 = completionRule3(inputURule3, type3Axioms) //Rule3
      println("----Completed rule3----")
      
      
      var inputURule4 = inputURule3 //no change in U after rule 3
      var inputRRule4 = sc.union(currDeltaRAllRules,currDeltaRRule3) 
      var currDeltaURule4 = completionRule4_new(inputURule4, inputRRule4, type4Axioms) // Rule4
      println("----Completed rule4----")

      var inputRRule5 = inputRRule4 //no change in R after rule 4
      var currDeltaRRule5 = completionRule5(inputRRule5, type5Axioms) //Rule5      
      println("----Completed rule5----")

      var inputRRule6 = sc.union(inputRRule5, currDeltaRRule5)
      var currDeltaRRule6 = completionRule6(inputRRule6, type6Axioms) //Rule6
      println("----Completed rule6----")
    
      //repartition U and R axioms   
      currDeltaURule1 = currDeltaURule1.repartition(numProcessors).cache()
      currDeltaURule2 = currDeltaURule2.repartition(numProcessors).cache()
      currDeltaRRule3 = currDeltaRRule3.repartition(numProcessors).cache()
      currDeltaURule4 = currDeltaURule4.repartition(numProcessors).cache()
      currDeltaRRule5 = currDeltaRRule5.repartition(numProcessors).cache()
      currDeltaRRule6 = currDeltaRRule6.repartition(numProcessors).cache()
      
      currDeltaUAllRulesCount = currDeltaURule1.count+currDeltaURule2.count+currDeltaURule4.count
      println("------Completed uAxioms count--------")
      
      currDeltaRAllRulesCount = currDeltaRRule3.count+currDeltaRRule5.count+currDeltaRRule6.count
      println("------Completed rAxioms count--------")
      
      //store all old and new uAxioms and rAxioms
      currDeltaUAllRules = sc.union(currDeltaUAllRules,currDeltaURule1,currDeltaURule2,currDeltaURule4).repartition(numProcessors)
      currDeltaRAllRules = sc.union(currDeltaRAllRules,currDeltaRRule3,currDeltaRRule5,currDeltaRRule6).repartition(numProcessors)
      
      var t_endLoop = System.nanoTime()
      
      //debugging
      println("===================================debug info=========================================")
      println("End of loop: 1. uAxioms count: " + currDeltaUAllRulesCount + ", rAxioms count: " + currDeltaRAllRulesCount)
      println("Runtime of the current loop: " + (t_endLoop - t_beginLoop) / 1e6 + " ms")
    
     
     //remaining iterations
     var counter = 1;
     while (currDeltaUAllRulesCount != 0 || currDeltaRAllRulesCount != 0) {

       var t_beginLoop = System.nanoTime()
       
       
       var prevDeltaURule1 = currDeltaURule1
       var prevDeltaURule2 = currDeltaURule2
       var prevDeltaRRule3 = currDeltaRRule3
       var prevDeltaURule4 = currDeltaURule4
       var prevDeltaRRule5 = currDeltaRRule5
       var prevDeltaRRule6 = currDeltaRRule6
       
      //loop counter 
       counter = counter + 1

       var inputURule1 = sc.union(prevDeltaURule1,prevDeltaURule2,prevDeltaURule4)
       currDeltaURule1 = completionRule1(inputURule1, type1Axioms) //Rule1
       println("----Completed rule1----")
     
      //build input to rule 2
      // var inputURule2 = sc.union(prevDeltaURule2,prevDeltaURule4, currDeltaURule1)      
       //debugging
       var inputURule2 = sc.union(currDeltaUAllRules,currDeltaURule1).distinct.repartition(numProcessors)
       currDeltaURule2 = completionRule2(inputURule2, type2Axioms) //Rule2
       println("----Completed rule2----")
      
      
      var inputURule3 = sc.union(prevDeltaURule4,currDeltaURule1,currDeltaURule2)
      currDeltaRRule3 = completionRule3(inputURule3, type3Axioms) //Rule3
      println("----Completed rule3----")      

      //var inputRRule4 = sc.union(prevDeltaRRule5,prevDeltaRRule6,currDeltaRRule3)
      //var inputURule4 = inputURule3 //no change in U after rule 3      
      //debugging
      var inputRRule4 = sc.union(currDeltaRAllRules,currDeltaRRule3).distinct.repartition(numProcessors)
      var inputURule4 = sc.union(inputURule2,currDeltaURule2).repartition(numProcessors)
      currDeltaURule4 = completionRule4_new(inputURule4, inputRRule4, type4Axioms) // Rule4
      println("----Completed rule4----")

      var inputRRule5 = inputRRule4 //no change in R after rule 4
      currDeltaRRule5 = completionRule5(inputRRule5, type5Axioms) //Rule5      
      println("----Completed rule5----")

     // var inputRRule6 = sc.union(prevDeltaRRule6, currDeltaRRule3, currDeltaRRule5)      
      //debugging
      var inputRRule6 = sc.union(inputRRule4,currDeltaRRule5).distinct.repartition(numProcessors)
      currDeltaRRule6 = completionRule6(inputRRule6, type6Axioms) //Rule6      
      println("----Completed rule6----")
      
      
      //collect all uAxioms and all rAxioms and count them. 
//      currDeltaUAllRules = sc.union(currDeltaURule1,currDeltaURule2,currDeltaURule4)
//      currDeltaRAllRules = sc.union(currDeltaRRule3,currDeltaRRule5,currDeltaRRule6)
      
      //repartition U and R axioms   
      currDeltaURule1 = currDeltaURule1.repartition(numProcessors).cache()
      currDeltaURule2 = currDeltaURule2.repartition(numProcessors).cache()
      currDeltaRRule3 = currDeltaRRule3.repartition(numProcessors).cache()
      currDeltaURule4 = currDeltaURule4.repartition(numProcessors).cache()
      currDeltaRRule5 = currDeltaRRule5.repartition(numProcessors).cache()
      currDeltaRRule6 = currDeltaRRule6.repartition(numProcessors).cache()
      
      var currDeltaU = sc.union(currDeltaURule1,currDeltaURule2,currDeltaURule4).distinct.repartition(numProcessors)
      currDeltaUAllRulesCount = currDeltaU.count // gives current count as it gets rid of common axioms generated by different rules
     // currDeltaUAllRulesCount = currDeltaURule1.count+currDeltaURule2.count+currDeltaURule4.count
      println("------Completed uAxioms count--------")
      
      var currDeltaR = sc.union(currDeltaRRule3,currDeltaRRule5,currDeltaRRule6).distinct.repartition(numProcessors)
      //currDeltaRAllRulesCount = currDeltaRRule3.count+currDeltaRRule5.count+currDeltaRRule6.count
      currDeltaRAllRulesCount = currDeltaR.count
      println("------Completed rAxioms count--------")

      //store the union of all new axioms 
       currDeltaUAllRules = sc.union(currDeltaUAllRules,currDeltaURule1,currDeltaURule2,currDeltaURule4).repartition(numProcessors)
       currDeltaRAllRules = sc.union(currDeltaRAllRules,currDeltaRRule3,currDeltaRRule5,currDeltaRRule6).repartition(numProcessors)
       
      //time
      var t_endLoop = System.nanoTime()
      
      //debugging
      println("===================================debug info=========================================")
      println("End of loop: " + counter + ". New uAxioms count: " + currDeltaUAllRulesCount + ", New rAxioms count: " + currDeltaRAllRulesCount)
      println("Runtime of the current loop: " + (t_endLoop - t_beginLoop) / 1e6 + " ms")
      //println("uAxiomsFinal dependencies: "+ uAxiomsFinal.toDebugString)
      //println("rAxiomsFinal dependencies: "+ rAxiomsFinal.toDebugString)
      println("======================================================================================")

    } //end of loop

    //union the new uAxioms with original input uAxioms. To verify closure count 
    //currDeltaUAllRules = sc.union(currDeltaUAllRules,uAxioms).repartition(numProcessors)
     
    //if already unioned with input uAxioms, repartion and do count
     currDeltaUAllRules= currDeltaUAllRules.repartition(numProcessors).cache
    println("Closure computed. Final number of uAxioms: " + currDeltaUAllRules.count)
    //uAxiomsFinal.foreach(println(_))
    //      for (sAxiom <- uAxiomsFinal) println(sAxiom._2+"|"+sAxiom._1)
    val t_end = System.nanoTime()

    //collect result (new subclass axioms) into 1 partition and spit out the result to a file.
    val sAxioms = currDeltaUAllRules.map({ case (v1, v2) => v2 + "|" + v1 }) // invert uAxioms to sAxioms
    sAxioms.coalesce(1, true).saveAsTextFile(args(1)) // coalesce to 1 partition so output can be written to 1 file
    println("Total runtime of the program: " + (t_end - t_init) / 1e6 + " ms")
    sc.stop()
  }
  
}