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
import org.apache.spark.HashPartitioner
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI

/**
 * Uses the current code of SparkELConfigTest for testing HDFS usage
 * 
 * @author Raghava Mutharaju
 */
object SparkELHDFSTest {

  private var numPartitions = -1
  
  /*
   * Initializes all the RDDs corresponding to each axiom-type. 
   */
  def initializeRDD(sc: SparkContext, dirPath: String) = {
//    require(numPartitions != -1, "set numPartitions before calling this method")
    // set numPartitions before calling this method
    val hashPartitioner = new HashPartitioner(numPartitions)
    val uAxioms = sc.textFile(dirPath + "sAxioms.txt").map[(Int, Int)](line => { 
      line.split("\\|") match { case Array(x, y) => (y.toInt, x.toInt) } })
      .partitionBy(hashPartitioner).persist()
    val rAxioms: RDD[(Int, (Int, Int))] = sc.emptyRDD
    val type1Axioms = sc.textFile(dirPath + "Type1Axioms.txt")
                      .map[(Int, Int)](line => { line.split("\\|") match { 
                        case Array(x, y) => (x.toInt, y.toInt) } })
                      .partitionBy(hashPartitioner).persist()
    val type2Axioms = sc.textFile(dirPath + "Type2Axioms.txt")
                      .map[(Int, (Int, Int))](line => { line.split("\\|") match { 
                        case Array(x, y, z) => (x.toInt, (y.toInt, z.toInt)) } })
                      .partitionBy(hashPartitioner).persist()
    val type3Axioms = sc.textFile(dirPath + "Type3Axioms.txt")
                      .map[(Int, (Int, Int))](line => { line.split("\\|") match { 
                        case Array(x, y, z) => (x.toInt, (y.toInt, z.toInt)) } })
                      .partitionBy(hashPartitioner).persist()
    val type4Axioms = sc.textFile(dirPath + "Type4Axioms.txt")
                      .map[(Int, (Int ,Int))](line => { line.split("\\|") match { 
                        case Array(x, y, z) => (x.toInt, (y.toInt, z.toInt)) } })
                      .partitionBy(hashPartitioner).persist()
    val type5Axioms = sc.textFile(dirPath + "Type5Axioms.txt")
                      .map[(Int, Int)](line => { line.split("\\|") match { 
                        case Array(x, y) => (x.toInt, y.toInt) } })
                      .partitionBy(hashPartitioner).persist()
    val type6Axioms = sc.textFile(dirPath + "Type6Axioms.txt")
                      .map[(Int, (Int, Int))](line => { line.split("\\|") match { 
                        case Array(x, y, z) => (x.toInt, (y.toInt, z.toInt)) } })
                      .partitionBy(hashPartitioner).persist()

    //return the initialized RDDs as a Tuple object (can at max have 22 elements in Spark Tuple)
    (uAxioms, rAxioms, type1Axioms, type2Axioms, type3Axioms, type4Axioms, type5Axioms, type6Axioms)
  }

  //completion rule1
  def completionRule1(uAxioms: RDD[(Int, Int)], type1Axioms: RDD[(Int, Int)]): RDD[(Int, Int)] = {

    val r1Join = type1Axioms.join(uAxioms, numPartitions).map({ case (k, v) => v })
    // uAxioms is immutable as it is input parameter
    val uAxiomsNew = uAxioms.union(r1Join).distinct.partitionBy(type1Axioms.partitioner.get) 
    uAxiomsNew
  }

  //completion rule 2
  def completionRule2(uAxioms: RDD[(Int, Int)], type2Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, Int)] = {

    var t_begin = System.nanoTime()
    val r2Join1 = type2Axioms.join(uAxioms, numPartitions)
    val r2Join1_count = r2Join1.persist(StorageLevel.MEMORY_ONLY_SER).count
    var t_end = System.nanoTime()
    println("r2Join1: type2Axioms.join(uAxioms). Count= " +r2Join1_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    t_begin = System.nanoTime()
    val r2Join1Remapped = r2Join1.map({ case (k, ((v1, v2), v3)) => (v1, (v2, v3)) })
    val r2Join2 = r2Join1Remapped.join(uAxioms, numPartitions)
    val r2Join2_count = r2Join2.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("r2Join2: r2Join1.map().join(uAxioms). Count= " +r2Join2_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    
    val r2JoinOutput = r2Join2.filter({ case (k, ((v1, v2), v3)) => v2 == v3 }).map({ case (k, ((v1, v2), v3)) => (v1, v2) })
    // uAxioms is immutable as it is input parameter
    
    t_begin = System.nanoTime()
    val uAxiomsNew = uAxioms.union(r2JoinOutput).distinct.partitionBy(type2Axioms.partitioner.get)
    val uAxiomsNew_count = uAxiomsNew.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("uAxiomsNew: uAxioms.union(r2Join.filter()). Count= " +uAxiomsNew_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    uAxiomsNew

  }

   def completionRule2_new(uAxioms: RDD[(Int, Int)], type2Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, Int)] = {

    var t_begin = System.nanoTime()
    val r2Join1 = type2Axioms.join(uAxioms, numPartitions)
    val r2Join1_count = r2Join1.persist(StorageLevel.MEMORY_ONLY_SER).count
    var t_end = System.nanoTime()
    println("r2Join1: type2Axioms.join(uAxioms). Count= " +r2Join1_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    t_begin = System.nanoTime()
    val r2Join1Remapped = r2Join1.map({ case (k, ((v1, v2), v3)) => (v1, (v2, v3)) })
    val r2Join2 = r2Join1Remapped.join(uAxioms, numPartitions)
    val r2Join2_count = r2Join2.cache().count
    t_end = System.nanoTime()
    println("r2Join2: r2Join1.map().join(uAxioms). Count= " +r2Join2_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    
    val r2JoinOutput = r2Join2.filter({ case (k, ((v1, v2), v3)) => v2 == v3 }).map({ case (k, ((v1, v2), v3)) => (v1, v2) })
    // uAxioms is immutable as it is input parameter
    
    t_begin = System.nanoTime()
    val uAxiomsNew = uAxioms.union(r2JoinOutput).distinct.partitionBy(type2Axioms.partitioner.get)
    val uAxiomsNew_count = uAxiomsNew.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("uAxiomsNew: uAxioms.union(r2Join.filter()). Count= " +uAxiomsNew_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
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
    val rAxiomsNew = rAxioms.union(r3Output).distinct().partitionBy(type3Axioms.partitioner.get)
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
  
   def completionRule4_Raghava(filteredUAxioms: RDD[(Int, Int)], uAxioms: RDD[(Int, Int)], rAxioms: RDD[(Int, (Int, Int))], type4Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, Int)] = {

    var t_begin_filter = System.nanoTime()
    val filteredUAxiomsCount = filteredUAxioms.cache().count()    
    var t_end_filter = System.nanoTime()
    println("filteredUAxioms rule4: " + filteredUAxiomsCount+", Time:"+(t_end_filter - t_begin_filter) / 1e6 + " ms")
    
    var t_begin = System.nanoTime()
    val type4AxiomsFillerKey = type4Axioms.map({ case (r, (a, b)) => (a, (r, b)) })    
    val r4Join1 = type4AxiomsFillerKey.join(filteredUAxioms) //can be replaced by map, a better version than join. See: http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-amp-camp-2012-advanced-spark.pdf
    val r4Join1Count = r4Join1.persist(StorageLevel.MEMORY_ONLY_SER).count()
    var t_end = System.nanoTime()
    println("r4Join1: #Partitions = " + r4Join1.partitions.size + 
        " Size = " + SizeEstimator.estimate(r4Join1) + 
        " Count = " + r4Join1Count + 
        ", Time taken: " + (t_end - t_begin) / 1e6 + " ms")
        
    t_begin = System.nanoTime()    
    val r4Join1YKey = r4Join1.map({ case (a, ((r1, b), y)) => (y, (r1, b)) })
    val rAxiomsPairYKey = rAxioms.map({ case (r2, (x, y)) => (y, (r2, x)) })
    val r4Join2 = r4Join1YKey.join(rAxiomsPairYKey)
    val r4Join2Count = r4Join2.persist(StorageLevel.MEMORY_ONLY_SER).count()
    t_end = System.nanoTime()
    println("r4Join2: #Partitions = " + r4Join2.partitions.size + 
        " Size = " + SizeEstimator.estimate(r4Join2) + 
        " Count = " + r4Join2Count + ", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    t_begin = System.nanoTime()
    val r4Result = r4Join2.filter({ case (y, ((r1, b), (r2, x))) => r1 == r2 })
                          .map({ case (y, ((r1, b), (r2, x))) => (b, x) })
    val r4ResultCount = r4Result.persist(StorageLevel.MEMORY_ONLY_SER).count()
    t_end = System.nanoTime()
    //debug
     println("r4ResultCount: #Partitions = " + r4Result.partitions.size + 
        " Size = " + SizeEstimator.estimate(r4Result) + 
        " Count=  " +r4ResultCount+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    
    t_begin = System.nanoTime()
    val uAxiomsNew = uAxioms.union(r4Result).distinct.partitionBy(type4Axioms.partitioner.get)  
    val uAxiomsNewCount = uAxiomsNew.cache().count
    t_end = System.nanoTime()
    println("uAxioms-union: #Partitions = " + uAxiomsNew.partitions.size + 
        " Size = " + SizeEstimator.estimate(uAxiomsNew) + 
        " Count=  " +uAxiomsNewCount+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    uAxiomsNew
  }
   
   def completionRule4_new(filteredUAxioms: RDD[(Int, Int)], uAxioms: RDD[(Int, Int)], rAxioms: RDD[(Int, (Int, Int))], type4Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, Int)] = {

    println("Debugging with persist(StorageLevel.MEMORY_ONLY_SER)")  
    var t_begin = System.nanoTime()
    val type4AxiomsFillerKey = type4Axioms.map({ case (r, (a, b)) => (a, (r, b)) })
    val r4Join1 = type4AxiomsFillerKey.join(uAxioms) //can be replaced by map, a better version than join. See: http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-amp-camp-2012-advanced-spark.pdf
    val r4Join1Count = r4Join1.persist(StorageLevel.MEMORY_ONLY_SER).count()
    var t_end = System.nanoTime()
    println("r4Join1: #Partitions = " + r4Join1.partitions.size + 
        " Size = " + SizeEstimator.estimate(r4Join1) + 
        " Count = " + r4Join1Count + 
        ", Time taken: " + (t_end - t_begin) / 1e6 + " ms")
    
    
    println("*****rAxioms count before join2: "+rAxioms.count())
    t_begin = System.nanoTime()    
    val r4Join1YKey = r4Join1.map({ case (a, ((r, b), y)) => (r, (b, y)) })
    val rAxiomsPairYKey = rAxioms.map({ case (r, (x, y)) => (r, (x, y)) }) //no change actually
    val r4Join2 = r4Join1YKey.join(rAxiomsPairYKey)
    val r4Join2Count = r4Join2.persist(StorageLevel.MEMORY_ONLY_SER).count()
    t_end = System.nanoTime()
    println("r4Join2: #Partitions = " + r4Join2.partitions.size + 
        " Size = " + SizeEstimator.estimate(r4Join2) + 
        " Count = " + r4Join2Count + ", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    t_begin = System.nanoTime()
    val r4Result = r4Join2.filter({ case (r, ((b,y1), (x, y2))) => y1 == y2 })
                          .map({ case (r, ((b,y1), (x, y2))) => (b, x) })
    val r4ResultCount = r4Result.persist(StorageLevel.MEMORY_ONLY_SER).count()
    t_end = System.nanoTime()
    //debug
     println("r4ResultCount: #Partitions = " + r4Result.partitions.size + 
        " Size = " + SizeEstimator.estimate(r4Result) + 
        " Count=  " +r4ResultCount+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    
    t_begin = System.nanoTime()
    val uAxiomsNew = uAxioms.union(r4Result).distinct.partitionBy(type4Axioms.partitioner.get)  
    val uAxiomsNewCount = uAxiomsNew.cache().count
    t_end = System.nanoTime()
    println("uAxioms-union: #Partitions = " + uAxiomsNew.partitions.size + 
        " Size = " + SizeEstimator.estimate(uAxiomsNew) + 
        " Count=  " +uAxiomsNewCount+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    uAxiomsNew
  }

  //completion rule 5
  def completionRule5(rAxioms: RDD[(Int, (Int, Int))], type5Axioms: RDD[(Int, Int)]): RDD[(Int, (Int, Int))] = {

    val r5Join = type5Axioms.join(rAxioms, numPartitions).map({ case (k, (v1, (v2, v3))) => (v1, (v2, v3)) })
    val rAxiomsNew = rAxioms.union(r5Join).distinct.partitionBy(type5Axioms.partitioner.get)

    rAxiomsNew
  }

  //completion rule 6
  def completionRule6(rAxioms: RDD[(Int, (Int, Int))], type6Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {

    val r6Join1 = type6Axioms.join(rAxioms, numPartitions).map({ 
      case (k, ((v1, v2), (v3, v4))) => (v1, (v2, (v3, v4))) })
    val r6Join2 = r6Join1.join(rAxioms, numPartitions).filter({ 
      case (k, ((v2, (v3, v4)), (v5, v6))) => v4 == v5 }).map({ 
        case (k, ((v2, (v3, v4)), (v5, v6))) => (v2, (v3, v6)) })
    val rAxiomsNew = rAxioms.union(r6Join2).distinct.partitionBy(type6Axioms.partitioner.get)

    rAxiomsNew
  }
  
  //completion rule 6
  def completionRule6_new(rAxioms: RDD[(Int, (Int, Int))], type6Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {

    var t_begin = System.nanoTime()
    val r6Join1 = type6Axioms.join(rAxioms).map({ case (k, ((v1, v2), (v3, v4))) => (v4, (v1, v2, v3)) })
    val r6Join1_count = r6Join1.persist(StorageLevel.MEMORY_ONLY_SER).count
    var t_end = System.nanoTime()
    println("r6Join1=type6Axioms.join(rAxioms).map(). Count= " +r6Join1_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    val rAxiomsReMapped = rAxioms.map({ case (r,(y,z)) => (y,(r,z))})
    
    t_begin = System.nanoTime()
    val r6Join2 = r6Join1.join(rAxiomsReMapped)
    val r6Join2_count = r6Join2.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("r6Join2=r6Join1.join(rAxioms). Count= " +r6Join2_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    t_begin = System.nanoTime()
    val r6Join2Filtered = r6Join2.filter({ case (k, ((v1,v2,v3),(r,z))) => v1 == r })
                                 .map({ case (k, ((v1,v2,v3),(r,z))) => (v2, (v3, z)) })
                                 .distinct
    val r6Join2FilteredCount = r6Join2Filtered.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("r6Join2.filter().map(). Count= " +r6Join2FilteredCount+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    val rAxiomsNew = rAxioms.union(r6Join2Filtered).distinct.partitionBy(type6Axioms.partitioner.get)
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
    
    val localFileScheme = "file:///"
    val hdfsFileScheme = "hdfs://"
    val fileSystemURI: URI = {
      if(args(1).startsWith(localFileScheme))
        new URI(localFileScheme)
      else if(args(1).startsWith(hdfsFileScheme))
        new URI(hdfsFileScheme + "/")
      else 
        null
    }
    // delete the output directory
    val hadoopConf = new Configuration()
    require(fileSystemURI != null, "Provide file:/// or hdfs:// for " + 
            "input/output directories")
    val fileSystem = FileSystem.get(fileSystemURI, hadoopConf)
    val dirDeleted = fileSystem.delete(new Path(args(1)), true)
    
    val numProcessors = Runtime.getRuntime.availableProcessors()
    numPartitions = numProcessors * args(2).toInt

    var (uAxioms, rAxioms, type1Axioms, type2Axioms, type3Axioms, 
        type4Axioms, type5Axioms, type6Axioms) = initializeRDD(sc, args(0))

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
    
    val type4Fillers = type4Axioms.collect().map({ 
                                  case (k, (v1, v2)) => v1 }).toSet
    val type4FillersBroadcast = sc.broadcast(type4Fillers)

    while (prevUAxiomsCount != currUAxiomsCount || prevRAxiomsCount != currRAxiomsCount) {

      var t_beginLoop = System.nanoTime()

      counter = counter + 1
      
      var t_begin_rule = System.nanoTime()
      var uAxiomsRule1 = completionRule1(uAxiomsFinal, type1Axioms) 
      uAxiomsRule1 = uAxiomsRule1.cache()
      var uAxiomRule1Count = uAxiomsRule1.count
      var t_end_rule = System.nanoTime()      
      println("----Completed rule1---- : ")
      println("count: "+ uAxiomRule1Count+" Time taken: "+ (t_end_rule - t_begin_rule) / 1e6 + " ms")
      println("=====================================")
      
      t_begin_rule = System.nanoTime()
      var uAxiomsRule2 = completionRule2(uAxiomsRule1, type2Axioms)
      //uAxiomsRule2 = uAxiomsRule2.cache()
      var uAxiomsRule2Count = uAxiomsRule2.count
      t_end_rule = System.nanoTime() 
      println("----Completed rule2----")
      println("count: "+ uAxiomsRule2Count+" Time taken: "+ (t_end_rule - t_begin_rule) / 1e6 + " ms")
      println("=====================================")

      //debugging - repartition before rule3
     // uAxiomsRule2 = uAxiomsRule2.repartition(numProcessors)
      
      t_begin_rule = System.nanoTime()
      var rAxiomsRule3 = completionRule3(uAxiomsRule2, rAxiomsFinal, type3Axioms) 
      rAxiomsRule3 = rAxiomsRule3.cache()
      var rAxiomsRule3Count = rAxiomsRule3.count
      t_end_rule = System.nanoTime() 
      println("----Completed rule3----")
      println("count: "+ rAxiomsRule3Count+" Time taken: "+ (t_end_rule - t_begin_rule) / 1e6 + " ms")
      println("======================================")
     
      
      val filteredUAxiomsRule2 = uAxiomsRule2.filter({ 
          case (k, v) => type4FillersBroadcast.value.contains(k) })
//      rAxiomsRule3.countByKey().foreach({ case (k, v) => println(k + ": " + v) })
          
      t_begin_rule = System.nanoTime()   
      var uAxiomsRule4 = completionRule4_Raghava(filteredUAxiomsRule2, uAxiomsRule2, 
          rAxiomsRule3, type4Axioms)
      uAxiomsRule4 = uAxiomsRule4.cache()
      var uAxiomsRule4Count = uAxiomsRule4.count
      t_end_rule = System.nanoTime() 
      println("----Completed rule4----")
      println("count: "+ uAxiomsRule4Count+" Time taken: "+ (t_end_rule - t_begin_rule) / 1e6 + " ms")
      println("=====================================")

      t_begin_rule = System.nanoTime()
      var rAxiomsRule5 = completionRule5(rAxiomsRule3, type5Axioms) 
      rAxiomsRule5 = rAxiomsRule5.cache()
      var rAxiomsRule5Count = rAxiomsRule5.count
      t_end_rule = System.nanoTime() 
      println("----Completed rule5----")
      println("count: "+ rAxiomsRule5Count+" Time taken: "+ (t_end_rule - t_begin_rule) / 1e6 + " ms")
      println("=====================================")

      
      t_begin_rule = System.nanoTime()
      var rAxiomsRule6 = completionRule6_new(rAxiomsRule5, type6Axioms) 
      rAxiomsRule6 = rAxiomsRule6.cache()
      var rAxiomsRule6Count = rAxiomsRule6.count
      t_end_rule = System.nanoTime() 
      println("----Completed rule6----")
      println("count: "+ rAxiomsRule6Count+" Time taken: "+ (t_end_rule - t_begin_rule) / 1e6 + " ms")
      println("====================================")

      uAxiomsFinal = uAxiomsRule4
      rAxiomsFinal = rAxiomsRule6 

//      uAxiomsFinal = uAxiomsFinal.partitionBy(type1Axioms.partitioner.get).persist()
//      rAxiomsFinal = rAxiomsFinal.partitionBy(type1Axioms.partitioner.get).persist()
      uAxiomsFinal = uAxiomsFinal.persist()
      rAxiomsFinal = rAxiomsFinal.persist()

      //update counts
      prevUAxiomsCount = currUAxiomsCount
      prevRAxiomsCount = currRAxiomsCount

      var t_begin_uAxiomCount = System.nanoTime() 
      currUAxiomsCount = uAxiomsFinal.count()
      var t_end_uAxiomCount = System.nanoTime()
      println("------Completed uAxioms count--------")
      println("Time taken for uAxiom count: "+ (t_end_uAxiomCount - t_begin_uAxiomCount) / 1e6 + " ms")
      println("====================================")
      
      var t_begin_rAxiomCount = System.nanoTime()
      currRAxiomsCount = rAxiomsFinal.count()
      var t_end_rAxiomCount = System.nanoTime()
      println("------Completed rAxioms count--------")
      println("Time taken for rAxiom count: "+ (t_end_rAxiomCount - t_begin_rAxiomCount) / 1e6 + " ms")      
      println("====================================")

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

//    }
    sc.stop()
  }
}
