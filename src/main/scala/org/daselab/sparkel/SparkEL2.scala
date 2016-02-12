package org.daselab.sparkel

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField

/**
 * EL completion rule implementation using the Datasets feature 
 * introduced in Spark 1.6
 * 
 * @author Raghava Mutharaju
 */
object SparkEL2 {
  
  private var rAxiomsInitialized = false
  
  def initialize(sqlContext: SQLContext, dirPath: String) = {   
    // provides encoders for case classes and primitive types
    import sqlContext.implicits._
    
    var sAxioms = sqlContext.read.json(dirPath + 
        "sAxioms.json").as[SAxiom].as("sAxioms")
    var rAxioms = sqlContext.emptyDataFrame.as[RAxiom]
    val type1Axioms = sqlContext.read.json(dirPath + 
        "Type1Axioms.json").as[Type1Axiom].as("type1Axioms")
    val type2Axioms = sqlContext.read.json(dirPath + 
        "Type2Axioms.json").as[Type2Axiom].as("type2Axioms")
    val type3Axioms = sqlContext.read.json(dirPath + 
        "Type3Axioms.json").as[Type3Axiom].as("type3Axioms")
    val type4Axioms = sqlContext.read.json(dirPath + 
        "Type4Axioms.json").as[Type4Axiom].as("type4Axioms")
    val type5Axioms = sqlContext.read.json(dirPath + 
        "Type5Axioms.json").as[Type5Axiom].as("type5Axioms")
    val type6Axioms = sqlContext.read.json(dirPath + 
        "Type6Axioms.json").as[Type6Axiom].as("type6Axioms")

    //return the initialized Datasets as a Tuple object (can at max have 22 elements in Spark Tuple)
    (sAxioms, rAxioms, type1Axioms, type2Axioms, type3Axioms, type4Axioms, type5Axioms, type6Axioms)
  }
  
  //completion rule1
  def completionRule1(sAxioms: Dataset[SAxiom], 
      type1Axioms: Dataset[Type1Axiom]): Dataset[SAxiom] = {
    val sqlContext = type1Axioms.sqlContext
    import sqlContext.implicits._
    val r1Join = type1Axioms.joinWith(sAxioms, 
        $"type1Axioms.subConcept" === $"sAxioms.superConcept")
        .map({ case (x, y) => SAxiom(y.subConcept, x.superConcept) })
    val sAxiomsNew = sAxioms.union(r1Join).distinct.as("sAxioms") // sAxioms is immutable as it is input parameter

    sAxiomsNew
  }
  
  //completion rule 2
  def completionRule2(sAxioms: Dataset[SAxiom], 
      type2Axioms: Dataset[Type2Axiom]): Dataset[SAxiom] = {
    val sqlContext = type2Axioms.sqlContext
    import sqlContext.implicits._
    val r2Join1 = type2Axioms.joinWith(sAxioms, 
        $"type2Axioms.leftConjunct1" === $"sAxioms.superConcept").as("r2Join1")
    val r2Join2 = r2Join1.joinWith(sAxioms, 
        $"r2Join1._1.leftConjunct2" === $"sAxioms.superConcept" && 
        $"r2Join1._2.subConcept" === $"sAxioms.subConcept")
        .map({ case (x, y) => SAxiom(x._2.subConcept, x._1.superConcept) })
    val sAxiomsNew = sAxioms.union(r2Join2).distinct.as("sAxioms") // sAxioms is immutable as it is input parameter

    sAxiomsNew
  }
  
  //completion rule 3
  def completionRule3(sAxioms: Dataset[SAxiom], rAxioms: Dataset[RAxiom], 
      type3Axioms: Dataset[Type3Axiom]): Dataset[RAxiom] = {
    val sqlContext = type3Axioms.sqlContext
    import sqlContext.implicits._     
    val r3Join = type3Axioms.joinWith(sAxioms, 
        $"type3Axioms.subConcept" === $"sAxioms.superConcept")
    val r3Output = r3Join.map({ case (x, y) => 
      RAxiom(x.rhsRole, y.subConcept, x.rhsFiller) })
    if (!rAxiomsInitialized) {
      rAxiomsInitialized = true
      r3Output.as("rAxioms")
    }
    else
      rAxioms.union(r3Output).distinct.as("rAxioms")
  }
  
  //completion rule 4
  def completionRule4(sAxioms: Dataset[SAxiom], rAxioms: Dataset[RAxiom], 
      type4Axioms: Dataset[Type4Axiom]): Dataset[SAxiom] = {
    val sqlContext = type4Axioms.sqlContext
    import sqlContext.implicits._  
    val r4Join1 = type4Axioms.joinWith(rAxioms, 
        $"type4Axioms.lhsRole" === $"rAxioms.role").as("r4Join1")
    val r4Join2 = r4Join1.joinWith(sAxioms, 
        $"r4Join1._2.pairY" === $"sAxioms.subConcept" && 
        $"r4Join1._1.lhsFiller" === $"sAxioms.superConcept")
        .map({ case (x, y) => SAxiom(y.subConcept, y.superConcept) })
    val sAxiomsNew = sAxioms.union(r4Join2).distinct.as("sAxioms")

    sAxiomsNew
  }
  
  //completion rule 5
  def completionRule5(rAxioms: Dataset[RAxiom], 
      type5Axioms: Dataset[Type5Axiom]): Dataset[RAxiom] = {
    val sqlContext = type5Axioms.sqlContext
    import sqlContext.implicits._
    val r5Join = type5Axioms.joinWith(rAxioms, 
        $"type5Axioms.subRole" === $"rAxioms.role")
        .map({ case (x, y) => RAxiom(x.superRole, y.pairX, y.pairY) })
    val rAxiomsNew = rAxioms.union(r5Join).distinct.as("rAxioms")

    rAxiomsNew
  }
  
  //completion rule 6
  def completionRule6(rAxioms: Dataset[RAxiom], 
      type6Axioms: Dataset[Type6Axiom]): Dataset[RAxiom] = {
    val sqlContext = type6Axioms.sqlContext
    import sqlContext.implicits._
    val r6Join1 = type6Axioms.joinWith(rAxioms, 
        $"type6Axioms.lhsRole1" === $"rAxioms.role").as("r6Join1")
    val r6Join2 = r6Join1.joinWith(rAxioms, 
        $"r6Join1._1.lhsRole2" === $"rAxioms.role" && 
        $"r6Join1._2.pairY" === $"rAxioms.pairX")
        .map({ case (x, y) => RAxiom(x._1.superRole, x._2.pairX, y.pairY) })
    val rAxiomsNew = rAxioms.union(r6Join2).distinct.as("rAxioms")

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

    val conf = new SparkConf().setAppName("SparkEL2")
    conf.registerKryoClasses(Array(classOf[Type1Axiom], classOf[Type2Axiom], 
        classOf[Type3Axiom], classOf[Type4Axiom], classOf[Type5Axiom], 
        classOf[Type6Axiom], classOf[SAxiom], classOf[RAxiom], 
        classOf[StructType], classOf[StructField]))
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
    sAxiomsFinal.coalesce(1).foreach({axiom: SAxiom => 
      println(axiom.subConcept + "|" + axiom.superConcept)})
    println("Total runtime of the program: " + (t_end - t_init) / 1e6 + " ms")
    sc.stop()
  }
  
}
