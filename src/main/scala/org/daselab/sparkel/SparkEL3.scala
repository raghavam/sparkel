package org.daselab.sparkel

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

/**
 * EL completion rule implementation using DataFrames
 * 
 * @author Raghava Mutharaju
 */
object SparkEL3 {
  
  private var rAxiomsInitialized = false
  
  def initialize(sqlContext: SQLContext, dirPath: String) = {   
    val sAxioms = sqlContext.read.json(dirPath + "sAxioms.json")
    val rAxioms = sqlContext.emptyDataFrame
    val type1Axioms = sqlContext.read.json(dirPath + "Type1Axioms.json")
    val type2Axioms = sqlContext.read.json(dirPath + "Type2Axioms.json")
    val type3Axioms = sqlContext.read.json(dirPath + "Type3Axioms.json")
    val type4Axioms = sqlContext.read.json(dirPath + "Type4Axioms.json")
    val type5Axioms = sqlContext.read.json(dirPath + "Type5Axioms.json")
    val type6Axioms = sqlContext.read.json(dirPath + "Type6Axioms.json")

    //return the initialized Datasets as a Tuple object (can at max have 22 elements in Spark Tuple)
    (sAxioms, rAxioms, type1Axioms, type2Axioms, type3Axioms, 
        type4Axioms, type5Axioms, type6Axioms)
  }
  
  //completion rule1
  def completionRule1(sAxioms: DataFrame, type1Axioms: DataFrame): DataFrame = {
    val r1Join = type1Axioms.join(sAxioms, 
        type1Axioms.col(Type1Axiom.SubConcept) === sAxioms.col(SAxiom.SuperConcept))
        .select(sAxioms.col(SAxiom.SubConcept), type1Axioms.col(Type1Axiom.SuperConcept))
    val sAxiomsNew = sAxioms.unionAll(r1Join).distinct()
    sAxiomsNew
  }
  
  //completion rule 2
  def completionRule2(sAxioms: DataFrame, type2Axioms: DataFrame): DataFrame = {
    val r2Join1 = type2Axioms.join(sAxioms, 
        type2Axioms.col(Type2Axiom.LeftConjunct1) === sAxioms.col(SAxiom.SuperConcept))
        .select(type2Axioms.col(Type2Axiom.LeftConjunct2).as("leftc2"), 
                type2Axioms.col(Type2Axiom.SuperConcept).as("supc"), 
                sAxioms.col(SAxiom.SubConcept).as("subc"))
    val r2Join2 = r2Join1.join(sAxioms, 
        r2Join1.col("leftc2") === sAxioms.col(SAxiom.SuperConcept) && 
        r2Join1.col("subc") === sAxioms.col(SAxiom.SubConcept))
        .select(r2Join1.col("subc"), r2Join1.col("supc"))
    val sAxiomsNew = sAxioms.unionAll(r2Join2).distinct()
    sAxiomsNew
  }
  
  //completion rule 3
  def completionRule3(sAxioms: DataFrame, rAxioms: DataFrame, 
      type3Axioms: DataFrame): DataFrame = {    
    val r3Join = type3Axioms.join(sAxioms, 
        type3Axioms.col(Type3Axiom.SubConcept) === sAxioms.col(SAxiom.SuperConcept))
        .select(type3Axioms.col(Type3Axiom.RHSRole).as(RAxiom.Role), 
                sAxioms.col(SAxiom.SubConcept).as(RAxiom.PairX), 
                type3Axioms.col(Type3Axiom.RHSFiller).as(RAxiom.PairY))
    if (!rAxiomsInitialized) {
      rAxiomsInitialized = true
      r3Join
    }
    else
      rAxioms.unionAll(r3Join).distinct()
  }
  
  //completion rule 4
  def completionRule4(sAxioms: DataFrame, rAxioms: DataFrame, 
      type4Axioms: DataFrame): DataFrame = { 
    val r4Join1 = type4Axioms.join(rAxioms, 
        type4Axioms.col(Type4Axiom.LHSRole) === rAxioms.col(RAxiom.Role))
        .select(type4Axioms.col(Type4Axiom.LHSFiller), 
                type4Axioms.col(Type4Axiom.SuperConcept).as("supc"), 
                rAxioms.col(RAxiom.PairX), rAxioms.col(RAxiom.PairY))
    val r4Join2 = r4Join1.join(sAxioms, 
        r4Join1.col(RAxiom.PairY) === sAxioms.col(SAxiom.SubConcept) && 
        r4Join1.col(Type4Axiom.LHSFiller) === sAxioms.col(SAxiom.SuperConcept))
        .select(r4Join1.col(RAxiom.PairX), r4Join1.col("supc"))
    val sAxiomsNew = sAxioms.unionAll(r4Join2).distinct()
    sAxiomsNew
  }
  
  //completion rule 5
  def completionRule5(rAxioms: DataFrame, type5Axioms: DataFrame): DataFrame = {
    val r5Join = type5Axioms.join(rAxioms, 
        type5Axioms.col(Type5Axiom.SubRole) === rAxioms.col(RAxiom.Role))
        .select(type5Axioms.col(Type5Axiom.SuperRole), 
                rAxioms.col(RAxiom.PairX), rAxioms.col(RAxiom.PairY))
    val rAxiomsNew = rAxioms.unionAll(r5Join).distinct()
    rAxiomsNew
  }
  
  //completion rule 6
  def completionRule6(rAxioms: DataFrame, 
      type6Axioms: DataFrame): DataFrame = {
    val r6Join1 = type6Axioms.join(rAxioms, 
        type6Axioms.col(Type6Axiom.LHSRole1) === rAxioms.col(RAxiom.Role))
        .select(type6Axioms.col(Type6Axiom.LHSRole2), 
                type6Axioms.col(Type6Axiom.SuperRole), 
                rAxioms.col(RAxiom.PairX).as("pairX1"), 
                rAxioms.col(RAxiom.PairY).as("pairY1"))
    val r6Join2 = r6Join1.join(rAxioms, 
        r6Join1.col(Type6Axiom.LHSRole2) === rAxioms.col(RAxiom.Role) && 
        r6Join1.col("pairY1") === rAxioms.col(RAxiom.PairX))
        .select(r6Join1.col(Type6Axiom.SuperRole), r6Join1.col("pairX1"), 
                rAxioms.col(RAxiom.PairY))
    val rAxiomsNew = rAxioms.unionAll(r6Join2).distinct()
    rAxiomsNew
  }
  
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Missing args: 1. path of directory containing the " +  
          "axiom files, 2. output directory to save the computed sAxioms")
      System.exit(-1)
    }

    //init time
    val t_init = System.nanoTime()

    val conf = new SparkConf().setAppName("SparkEL3")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val (sAxioms, rAxioms, type1Axioms, type2Axioms, type3Axioms, type4Axioms, 
        type5Axioms, type6Axioms) = initialize(sqlContext, args(0))
        
    val type1AxiomsCount = type1Axioms.cache().count()
    val type2AxiomsCount = type2Axioms.cache().count()
    val type3AxiomsCount = type3Axioms.cache().count()
    val type4AxiomsCount = type4Axioms.cache().count()
    val type5AxiomsCount = type5Axioms.cache().count()
    val type6AxiomsCount = type6Axioms.cache().count()    
    sAxioms.cache()

    //compute closure
    var prevSAxiomsCount: Long = 0
    var prevRAxiomsCount: Long = 0
    var currSAxiomsCount: Long = sAxioms.count
    var currRAxiomsCount: Long = rAxioms.count

    println("Before closure computation. Initial uAxioms count: " + 
        currSAxiomsCount + ", Initial rAxioms count: " + currRAxiomsCount)
    var counter = 0;
    var sAxiomsFinal = sAxioms
    var rAxiomsFinal = rAxioms
    val numProcessors = Runtime.getRuntime.availableProcessors()

    // the last iteration is redundant but there is no way to avoid it
    while (prevSAxiomsCount != currSAxiomsCount || prevRAxiomsCount != currRAxiomsCount) {

      var t_beginLoop = System.nanoTime()
      counter = counter + 1
      
      val sAxiomsRule1 = { 
        if (type1AxiomsCount != 0) completionRule1(sAxiomsFinal, type1Axioms) 
        else sAxiomsFinal 
      }
//      sAxiomsRule1 = sAxiomsRule1.cache()
//      sAxiomsRule1.count()
      println("----Completed rule1----")

      val sAxiomsRule2 = { 
        if (type2AxiomsCount != 0) completionRule2(sAxiomsRule1, type2Axioms) 
        else sAxiomsRule1
      }
//      sAxiomsRule2 = sAxiomsRule2.cache()
//      sAxiomsRule2.count()
      println("----Completed rule2----")

      val rAxiomsRule3 = { 
        if (type3AxiomsCount != 0) completionRule3(sAxiomsRule2, rAxiomsFinal, type3Axioms) 
        else rAxiomsFinal
      }
//      rAxiomsRule3 = rAxiomsRule3.cache()
//      rAxiomsRule3.count()
      println("----Completed rule3----")

      val sAxiomsRule4 = { 
        if (type4AxiomsCount != 0) completionRule4(sAxiomsRule2, rAxiomsRule3, type4Axioms) 
        else sAxiomsRule2
      }
      println("----Completed rule4----")

      val rAxiomsRule5 = { 
        if (type5AxiomsCount != 0) completionRule5(rAxiomsRule3, type5Axioms) 
        else rAxiomsRule3
      } 
//      rAxiomsRule5 = rAxiomsRule5.cache()
//      rAxiomsRule5.count()
      println("----Completed rule5----")

      val rAxiomsRule6 = { 
        if (type6AxiomsCount != 0) completionRule6(rAxiomsRule5, type6Axioms) 
        else rAxiomsRule5
      }
      println("----Completed rule6----")

      sAxiomsFinal = sAxiomsRule4
      rAxiomsFinal = rAxiomsRule6

      sAxiomsFinal = sAxiomsFinal.repartition(numProcessors).cache()
      rAxiomsFinal = rAxiomsFinal.repartition(numProcessors).cache()

      //update counts
      prevSAxiomsCount = currSAxiomsCount
      prevRAxiomsCount = currRAxiomsCount
      currSAxiomsCount = sAxiomsFinal.count()
      currRAxiomsCount = rAxiomsFinal.count()

      //time
      var t_endLoop = System.nanoTime()

      //debugging
      println("===================================debug info=========================================")
      println("End of loop: " + counter + ". sAxioms count: " + 
          currSAxiomsCount + ", rAxioms count: " + currRAxiomsCount)
      println("Runtime of the current loop: " + (t_endLoop - t_beginLoop) / 1e6 + " ms")
      println("======================================================================================")

    } 

    println("Closure computed. Final number of uAxioms: " + currSAxiomsCount)
    val t_end = System.nanoTime()

    //collect result into 1 partition and spit out the result to a file.
    sAxiomsFinal.coalesce(1).write.json(args(1) + "saxioms-final.json")
    println("Total runtime of the program: " + (t_end - t_init) / 1e6 + " ms")
    sc.stop()
  }  
}
