package org.daselab.sparkel

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.rdd._
import java.io.File
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import main.scala.org.daselab.sparkel.Constants._

/**
 * Uses the current code of SparkEL for testing certain configuration 
 * parameters. 
 * 
 * @author Raghava Mutharaju
 */
object SparkELConfigTest {

  private var numPartitions = 8
  
  /*
   * Initializes all the RDDs corresponding to each axiom-type. 
   */
  def initializeRDD(sc: SparkContext, dirPath: String) = {
    val uAxioms = sc.textFile(dirPath + "sAxioms.txt").map(line => { 
      line.split("\\|") match { case Array(x, y) => (y.toInt, x.toInt) } })
    val rAxioms: RDD[(Int, (Int, Int))] = sc.emptyRDD

    val type1Axioms = sc.textFile(dirPath + "Type1Axioms.txt")
                      .map(line => { line.split("\\|") match { 
                        case Array(x, y) => (x.toInt, y.toInt) } })
    val type2Axioms = sc.textFile(dirPath + "Type2Axioms.txt")
                      .map(line => { line.split("\\|") match { 
                        case Array(x, y, z) => (x.toInt, (y.toInt, z.toInt)) } })
    val type3Axioms = sc.textFile(dirPath + "Type3Axioms.txt")
                      .map(line => { line.split("\\|") match { 
                        case Array(x, y, z) => (x.toInt, (y.toInt, z.toInt)) } })
    val type4Axioms = sc.textFile(dirPath + "Type4Axioms.txt")
                      .map(line => { line.split("\\|") match { 
                        case Array(x, y, z) => (x.toInt, (y.toInt, z.toInt)) } })
    val type5Axioms = sc.textFile(dirPath + "Type5Axioms.txt")
                      .map(line => { line.split("\\|") match { 
                        case Array(x, y) => (x.toInt, y.toInt) } })
    val type6Axioms = sc.textFile(dirPath + "Type6Axioms.txt")
                      .map(line => { line.split("\\|") match { 
                        case Array(x, y, z) => (x.toInt, (y.toInt, z.toInt)) } })

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

    val r1Join = type1Axioms.join(uAxioms, numPartitions).map({ case (k, v) => v })
    val uAxiomsNew = uAxioms.union(r1Join).distinct // uAxioms is immutable as it is input parameter

    uAxiomsNew
  }

  //completion rule 2
  def completionRule2(uAxioms: RDD[(Int, Int)], type2Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, Int)] = {

    val r2Join1 = type2Axioms.join(uAxioms, numPartitions)
    val r2Join1Remapped = r2Join1.map({ case (k, ((v1, v2), v3)) => (v1, (v2, v3)) })
    val r2Join2 = r2Join1Remapped.join(uAxioms, numPartitions)
    val r2JoinOutput = r2Join2.filter({ case (k, ((v1, v2), v3)) => v2 == v3 }).map({ case (k, ((v1, v2), v3)) => (v1, v2) })
    val uAxiomsNew = uAxioms.union(r2JoinOutput).distinct // uAxioms is immutable as it is input parameter

    uAxiomsNew

  }

  //completion rule 3
  def completionRule3(uAxioms: RDD[(Int, Int)], rAxioms: RDD[(Int, (Int, Int))], type3Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {

   // var t_begin = System.nanoTime()
    val r3Join = type3Axioms.join(uAxioms, numPartitions)
   // r3Join.cache().count
   // var t_end = System.nanoTime()
   // println("type3Axioms.join(uAxioms): " + (t_end - t_begin) / 1e6 + " ms")
    
   // t_begin = System.nanoTime()
    val r3Output = r3Join.map({ case (k, ((v1, v2), v3)) => (v1, (v3, v2)) })
   // r3Output.cache().count
   // t_end = System.nanoTime()
   // println("r3Join.map(...): " + (t_end - t_begin) / 1e6 + " ms")
    
   // t_begin = System.nanoTime()
    val rAxiomsNew = rAxioms.union(r3Output).distinct()
   // rAxioms.cache().count
   // t_end = System.nanoTime()
   // println("rAxioms.union(r3Output).distinct(): " + (t_end - t_begin) / 1e6 + " ms")
    
    rAxiomsNew

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
    val r4Join2Filtered = r4Join2.filter({ case (k, ((v2, (v3, v4)), v5)) => v4 == v5 }).map({ case (k, ((v2, (v3, v4)), v5)) => (v2, v3) })
    val r4Join2Filtered_count = r4Join2Filtered.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("r4Join2.filter().map(). Count = " +r4Join2Filtered_count +", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    t_begin = System.nanoTime()
    val uAxiomsNew = uAxioms.union(r4Join2Filtered).distinct
    val uAxiomsNew_count = uAxiomsNew.cache().count
    t_end = System.nanoTime()
    println("uAxioms.union(r4Join2Filtered).distinct. Count=  " +uAxiomsNew_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    uAxiomsNew
  }
  
   def completionRule4_new(uAxioms: RDD[(Int, Int)], 
       rAxioms: RDD[(Int, (Int, Int))], type4Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, Int)] = {

    println("Debugging with persist(StorageLevel.MEMORY_ONLY_SER)")
    
    var t_begin = System.nanoTime()
    val r4Join1 = type4Axioms.join(rAxioms, numPartitions)
    val r4Join1_count = r4Join1.persist(StorageLevel.MEMORY_ONLY_SER).count
    var t_end = System.nanoTime()
    println("type4Axioms.join(rAxioms): #Partitions = " + r4Join1.partitions.size + 
        " Size = " + SizeEstimator.estimate(r4Join1) + 
        " Count = " + r4Join1_count + 
        ", Time taken: " + (t_end - t_begin) / 1e6 + " ms")
    
    t_begin = System.nanoTime()
    val r4Join1ReMapped = r4Join1.map({ case (k, ((v1, v2), (v3, v4))) => (v4, (v2, (v3, v1))) })
    val r4Join1ReMapped_count = r4Join1ReMapped.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("r4Join1.map(...): #Partitions = " + r4Join1ReMapped.partitions.size + 
        " Size = " + SizeEstimator.estimate(r4Join1ReMapped) + 
        "Count = " + r4Join1ReMapped_count + 
        ", Time taken: "+ (t_end - t_begin) / 1e6 + " ms")
    
    val uAxiomsFlipped = uAxioms.map({ case (k1, v5) => (v5, k1) })
    
    t_begin = System.nanoTime()
    val r4Join2 = r4Join1ReMapped.join(uAxiomsFlipped, numPartitions)
    val r4Join2_count = r4Join2.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("r4Join1ReMapped.join(uAxioms): #Partitions = " + r4Join2.partitions.size + 
        " Size = " + SizeEstimator.estimate(r4Join2) + 
        " Count = " + r4Join2_count + ", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
   
    t_begin = System.nanoTime()
    val r4Join2Filtered = r4Join2.filter({ case (k, ((v2, (v3, v1)), k1)) => v1 == k1 }).map({ case (k, ((v2, (v3, v1)), k1)) => (v2, v3) })
    val r4Join2Filtered_count = r4Join2Filtered.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("r4Join2.filter().map(): #Partitions = " + r4Join2Filtered.partitions.size + 
        " Size = " + SizeEstimator.estimate(r4Join2Filtered) + 
        " Count = " +r4Join2Filtered_count +", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    t_begin = System.nanoTime()
    val uAxiomsNew = uAxioms.union(r4Join2Filtered).distinct
    val uAxiomsNew_count = uAxiomsNew.cache().count
    t_end = System.nanoTime()
    println("uAxioms.union(r4Join2Filtered).distinct: #Partitions = " + uAxiomsNew.partitions.size + 
        " Size = " + SizeEstimator.estimate(uAxiomsNew) + 
        " Count=  " +uAxiomsNew_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    uAxiomsNew
  }

  //completion rule 5
  def completionRule5(rAxioms: RDD[(Int, (Int, Int))], type5Axioms: RDD[(Int, Int)]): RDD[(Int, (Int, Int))] = {

    val r5Join = type5Axioms.join(rAxioms, numPartitions).map({ case (k, (v1, (v2, v3))) => (v1, (v2, v3)) })
    val rAxiomsNew = rAxioms.union(r5Join).distinct

    rAxiomsNew
  }

  //completion rule 6
  def completionRule6(rAxioms: RDD[(Int, (Int, Int))], type6Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {

    val r6Join1 = type6Axioms.join(rAxioms, numPartitions).map({ 
      case (k, ((v1, v2), (v3, v4))) => (v1, (v2, (v3, v4))) })
    val r6Join2 = r6Join1.join(rAxioms, numPartitions).filter({ 
      case (k, ((v2, (v3, v4)), (v5, v6))) => v4 == v5 }).map({ 
        case (k, ((v2, (v3, v4)), (v5, v6))) => (v2, (v3, v6)) })
    val rAxiomsNew = rAxioms.union(r6Join2).distinct

    rAxiomsNew
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
    if (args.length != 3) {
      System.err.println("Missing args:\n\t 1. path of directory containing " + 
              "the axiom files \n\t 2. output directory to save the computed " + 
              "sAxioms \n\t 3. Number of worker nodes in the cluster")
      System.exit(-1)
    }

    //init time
    val t_init = System.nanoTime()

    val conf = new SparkConf().setAppName("SparkEL")
    val sc = new SparkContext(conf)
    
    val numProcessors = Runtime.getRuntime.availableProcessors()
    numPartitions = numProcessors * args(2).toInt
    //      sc.setCheckpointDir(CheckPointDir) //set checkpoint directory. See directions here: https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-rdd-checkpointing.html

    var (uAxioms, rAxioms, type1Axioms, type2Axioms, type3Axioms, 
        type4Axioms, type5Axioms, type6Axioms) = initializeRDD(sc, args(0))
    uAxioms = uAxioms.cache()

    //compute closure
    var prevUAxiomsCount: Long = 0
    var prevRAxiomsCount: Long = 0
    var currUAxiomsCount: Long = uAxioms.count
    var currRAxiomsCount: Long = rAxioms.count

    println("Before closure computation. Initial uAxioms count: " + 
        currUAxiomsCount + ", Initial rAxioms count: " + currRAxiomsCount)
    var counter = 0;
    var uAxiomsFinal = uAxioms
    var rAxiomsFinal = rAxioms
    

    while (prevUAxiomsCount != currUAxiomsCount || prevRAxiomsCount != currRAxiomsCount) {

      var t_beginLoop = System.nanoTime()

      //debugging 
      counter = counter + 1
      
      var uAxiomsRule1 = completionRule1(uAxiomsFinal, type1Axioms) //Rule1
    //  uAxiomsRule1 = uAxiomsRule1.cache()
    //  uAxiomsRule1.count()
      println("----Completed rule1----")
     
      var uAxiomsRule2 = completionRule2(uAxiomsRule1, type2Axioms) //Rule2
    //  uAxiomsRule2 = uAxiomsRule2.cache()
    //  uAxiomsRule2.count()
      println("----Completed rule2----")

      //debugging - repartition before rule3
     // uAxiomsRule2 = uAxiomsRule2.repartition(numProcessors)
      
      var rAxiomsRule3 = completionRule3(uAxiomsRule2, rAxiomsFinal, type3Axioms) //Rule3
     // rAxiomsRule3 = rAxiomsRule3.cache()
     // rAxiomsRule3.count()
      println("----Completed rule3----")
      

      var uAxiomsRule4 = completionRule4_new(uAxiomsRule2, rAxiomsRule3, type4Axioms) // Rule4
     // uAxiomsRule4 = uAxiomsRule4.cache()
    //  uAxiomsRule4.count()
      println("----Completed rule4----")

      var rAxiomsRule5 = completionRule5(rAxiomsRule3, type5Axioms) //Rule5      
      //rAxiomsRule5 = rAxiomsRule5.cache()
      //rAxiomsRule5.count()
      println("----Completed rule5----")

      var rAxiomsRule6 = completionRule6(rAxiomsRule5, type6Axioms) //Rule6      
      //rAxiomsRule6 = rAxiomsRule6.cache()
     // rAxiomsRule6.count()
      println("----Completed rule6----")

      uAxiomsFinal = uAxiomsRule4
      rAxiomsFinal = rAxiomsRule5 //repalce after debug with rAxiomsRule6

      
      uAxiomsFinal = uAxiomsFinal.repartition(numPartitions).cache()
      rAxiomsFinal = rAxiomsFinal.repartition(numPartitions).cache()
      println("----Completed repartitions at end of loop----")

      //update counts
      prevUAxiomsCount = currUAxiomsCount
      prevRAxiomsCount = currRAxiomsCount

      currUAxiomsCount = uAxiomsFinal.count()
      println("------Completed uAxioms count--------")
      
      currRAxiomsCount = rAxiomsFinal.count()
      println("------Completed rAxioms count--------")

      //time
      var t_endLoop = System.nanoTime()

      //debugging
      println("===================================debug info=========================================")
      println("End of loop: " + counter + ". uAxioms count: " + 
          currUAxiomsCount + ", rAxioms count: " + currRAxiomsCount)
      println("Runtime of the current loop: " + (t_endLoop - t_beginLoop) / 1e6 + " ms")
      //println("uAxiomsFinal dependencies: "+ uAxiomsFinal.toDebugString)
      //println("rAxiomsFinal dependencies: "+ rAxiomsFinal.toDebugString)
      println("======================================================================================")

    } //end of loop

    println("Closure computed. Final number of uAxioms: " + currUAxiomsCount)
    //uAxiomsFinal.foreach(println(_))
    //      for (sAxiom <- uAxiomsFinal) println(sAxiom._2+"|"+sAxiom._1)
    val t_end = System.nanoTime()

    //collect result into 1 partition and spit out the result to a file.
    val sAxioms = uAxiomsFinal.map({ case (v1, v2) => v2 + "|" + v1 }) // invert uAxioms to sAxioms
    sAxioms.coalesce(1, true).saveAsTextFile(args(1)) // coalesce to 1 partition so output can be written to 1 file
    println("Total runtime of the program: " + (t_end - t_init) / 1e6 + " ms")
    sc.stop()
  }
}
