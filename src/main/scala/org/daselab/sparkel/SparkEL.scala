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

    val r1Join = type1Axioms.join(uAxioms).map({ case (k, v) => v })
    val uAxiomsNew = uAxioms.union(r1Join).distinct // uAxioms is immutable as it is input parameter

    uAxiomsNew
  }

  //completion rule 2
  def completionRule2(uAxioms: RDD[(Int, Int)], type2Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, Int)] = {

    val r2Join1 = type2Axioms.join(uAxioms)
    val r2Join1Remapped = r2Join1.map({ case (k, ((v1, v2), v3)) => (v1, (v2, v3)) })
    val r2Join2 = r2Join1Remapped.join(uAxioms)
    val r2JoinOutput = r2Join2.filter({ case (k, ((v1, v2), v3)) => v2 == v3 }).map({ case (k, ((v1, v2), v3)) => (v1, v2) })
    val uAxiomsNew = uAxioms.union(r2JoinOutput).distinct // uAxioms is immutable as it is input parameter

    uAxiomsNew

  }

  //completion rule 3
  def completionRule3(uAxioms: RDD[(Int, Int)], rAxioms: RDD[(Int, (Int, Int))], type3Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {

    val r3Join = type3Axioms.join(uAxioms)
    val r3Output = r3Join.map({ case (k, ((v1, v2), v3)) => (v1, (v3, v2)) })
    //val rAxiomsNew = rAxioms.union(r3Output).distinct()

    r3Output

  }

  //completion rule 4
  def completionRule4(uAxioms: RDD[(Int, Int)], rAxioms: RDD[(Int, (Int, Int))], type4Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, Int)] = {

    val r4Join1 = type4Axioms.join(rAxioms).map({ case (k, ((v1, v2), (v3, v4))) => (v1, (v2, (v3, v4))) })
    val r4Join2 = r4Join1.join(uAxioms).filter({ case (k, ((v2, (v3, v4)), v5)) => v4 == v5 }).map({ case (k, ((v2, (v3, v4)), v5)) => (v2, v3) })
    val uAxiomsNew = uAxioms.union(r4Join2).distinct

    uAxiomsNew
  }

  //completion rule 5
  def completionRule5(rAxioms: RDD[(Int, (Int, Int))], type5Axioms: RDD[(Int, Int)]): RDD[(Int, (Int, Int))] = {

    val r5Join = type5Axioms.join(rAxioms).map({ case (k, (v1, (v2, v3))) => (v1, (v2, v3)) })
    val rAxiomsNew = rAxioms.union(r5Join).distinct

    rAxiomsNew
  }

  //completion rule 6
  def completionRule6(rAxioms: RDD[(Int, (Int, Int))], type6Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {

    val r6Join1 = type6Axioms.join(rAxioms).map({ case (k, ((v1, v2), (v3, v4))) => (v1, (v2, (v3, v4))) })
    val r6Join2 = r6Join1.join(rAxioms).filter({ case (k, ((v2, (v3, v4)), (v5, v6))) => v4 == v5 }).map({ case (k, ((v2, (v3, v4)), (v5, v6))) => (v2, (v3, v6)) })
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
    
    uAxioms = uAxioms.repartition(numProcessors).cache()
   // rAxioms = rAxioms.repartition(numProcessors).cache()
    type1Axioms = type1Axioms.repartition(numProcessors).cache()
    type2Axioms = type2Axioms.repartition(numProcessors).cache()
    type3Axioms = type3Axioms.repartition(numProcessors).cache()
    type4Axioms = type4Axioms.repartition(numProcessors).cache()
    type5Axioms = type5Axioms.repartition(numProcessors).cache()
    type6Axioms = type6Axioms.repartition(numProcessors).cache()
    
    //should we materialize each type axioms? 
    type1Axioms.count
    type2Axioms.count
    type3Axioms.count
    type4Axioms.count
    type5Axioms.count
    type6Axioms.count
    
    //println("Before iteration uAxioms count: "+uAxioms.count())

    //compute closure
    var prevUAxiomsCount: Long = 0
    var prevRAxiomsCount: Long = 0
    var currUAxiomsCount: Long = uAxioms.count
    var currRAxiomsCount: Long = rAxioms.count

    println("Before closure computation. Initial uAxioms count: " + currUAxiomsCount + ", Initial rAxioms count: " + currRAxiomsCount)
    var counter = 0;
    var uAxiomsFinal = uAxioms
    var rAxiomsFinal = rAxioms
    

    while (prevUAxiomsCount != currUAxiomsCount || prevRAxiomsCount != currRAxiomsCount) {

      var t_beginLoop = System.nanoTime()

      //debugging 
      counter = counter + 1

      //        if(counter > 1)
      //        {
      //          uAxioms = sc.objectFile[(Int,Int)](CheckPointDir+"uAxiom"+(counter-1))
      //          rAxioms = sc.objectFile[(Int,(Int,Int))](CheckPointDir+"rAxiom"+(counter-1))
      //          
      //        }

      var uAxiomsRule1 = completionRule1(uAxiomsFinal, type1Axioms) //Rule1
      uAxiomsRule1 = uAxiomsRule1.cache()
      uAxiomsRule1.count()
      println("----Completed rule1----")
     
      
      //println("uAxiomsRule1 dependencies:\n"+uAxiomsRule1.toDebugString)
      //uAxiomsRule1.checkpoint()
      // uAxiomsRule1.count() // force action
      //println("uAxiomsRule1.isCheckpointed: "+uAxiomsRule1.isCheckpointed)

      var uAxiomsRule2 = completionRule2(uAxiomsRule1, type2Axioms) //Rule2
      uAxiomsRule2 = uAxiomsRule2.cache()
      uAxiomsRule2.count()
      println("----Completed rule2----")

      //debugging - repartition before rule3
     // uAxiomsRule2 = uAxiomsRule2.repartition(numProcessors)
      
      var rAxiomsRule3 = completionRule3(uAxiomsRule2, rAxiomsFinal, type3Axioms) //Rule3
      rAxiomsRule3 = rAxiomsRule3.cache()
      rAxiomsRule3.count()
      println("----Completed rule3----")
      

      var uAxiomsRule4 = completionRule4(uAxiomsRule2, rAxiomsRule3, type4Axioms) // Rule4
      uAxiomsRule4 = uAxiomsRule4.cache()
      uAxiomsRule4.count()
      println("----Completed rule4----")

      //optimization: 
      //Skip rules 5 and 6 which can't be triggered if rAxioms are not updated in previous loop or to this point in current loop

      //debug 
      //println("prevRAxiomsCount: "+prevRAxiomsCount+", currentRAxiomCount: "+currRAxiomsCount+", rAxioms.count: "+rAxiomsRule3.count)

      // if(prevRAxiomsCount != currRAxiomsCount || rAxiomsRule3.count > currRAxiomsCount){

      var rAxiomsRule5 = completionRule5(rAxiomsRule3, type5Axioms) //Rule5      
      rAxiomsRule5 = rAxiomsRule5.cache()
      rAxiomsRule5.count()
      println("----Completed rule5----")

      
//      rAxiomsRule5 = rAxiomsRule5.repartition(numProcessors).cache()
//      println("----Completed repartitions before Rule 6----")
      
      
      
//      var rAxiomsRule6 = completionRule6(rAxiomsRule5, type6Axioms) //Rule6      
//      rAxiomsRule6 = rAxiomsRule6.cache()
//      rAxiomsRule6.count()
//      println("----Completed rule6----")
      
      //        }
      //        else {
      //          println("Skipping Rules 5 and 6 since rAxiom was not updated in the previous loop or by Rule 3 in the current loop")
      //        }

      //        //debugging - add checkpointing to truncate lineage graph
      //        uAxioms.persist()
      //        rAxioms.persist()
      //        uAxioms.checkpoint()
      //        rAxioms.checkpoint()

      uAxiomsFinal = uAxiomsRule4
      rAxiomsFinal = rAxiomsRule5 //repalce after debug with rAxiomsRule6

      
      uAxiomsFinal = uAxiomsFinal.repartition(numProcessors).cache()
      rAxiomsFinal = rAxiomsFinal.repartition(numProcessors).cache()
      println("----Completed repartitions at end of loop----")

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
      println("------Completed uAxioms count--------")
      
      currRAxiomsCount = rAxiomsFinal.count()
      println("------Completed rAxioms count--------")

      //println("uAxiomsFinal.isCheckpointed inside loop: "+uAxiomsFinal.isCheckpointed)
      //println("rAxiomsFinal.isCheckpointed inside loop: "+rAxiomsFinal.isCheckpointed)

      //time
      var t_endLoop = System.nanoTime()
      
      //debug
      //numProcessors = numProcessors+5;

      //debugging
      println("===================================debug info=========================================")
      println("End of loop: " + counter + ". uAxioms count: " + currUAxiomsCount + ", rAxioms count: " + currRAxiomsCount)
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
