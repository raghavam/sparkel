package org.daselab.sparkel


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.rdd._
import java.io.File
import org.apache.spark.storage.StorageLevel
import main.scala.org.daselab.sparkel.Constants._
import org.apache.spark.util.SizeEstimator
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.HashPartitioner
import scala.collection.immutable.Set

object SparkELAlgoOpt{
  
  private var numPartitions = -1
  private val conf = new SparkConf().setAppName("SparkELAlgoOpt")
  private val sc = new SparkContext(conf)
  
  /*
   * Initializes all the RDDs corresponding to each axiom-type. 
   */
  def initializeRDD(sc: SparkContext, dirPath: String) = {
    require(numPartitions != -1, "set numPartitions before calling this method")
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
  def completionRule1(uAxioms: RDD[(Int, Int)], 
      type1Axioms: RDD[(Int, Int)]): RDD[(Int, Int)] = {

    val r1Join = type1Axioms.join(uAxioms).map({ case (k, v) => v })
                            .distinct
                            .partitionBy(type1Axioms.partitioner.get) 
   // val uAxiomsNew = uAxioms.union(r1Join).distinct // uAxioms is immutable as it is input parameter

    r1Join
  }

  //completion rule 2
  def completionRule2(uAxioms: RDD[(Int, Int)], 
      type2Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, Int)] = {
    val r2Join1 = type2Axioms.join(uAxioms)
    val r2Join1Remapped = r2Join1.map({ case (k, ((v1, v2), v3)) => (v1, (v2, v3)) })
    val r2Join2 = r2Join1Remapped.join(uAxioms)
    val r2JoinOutput = r2Join2.filter({ case (k, ((v1, v2), v3)) => v2 == v3 })
                              .map({ case (k, ((v1, v2), v3)) => (v1, v2) })
                              .distinct
                              .partitionBy(type2Axioms.partitioner.get)
    //val uAxiomsNew = uAxioms.union(r2JoinOutput).distinct // uAxioms is immutable as it is input parameter

    r2JoinOutput

  }

  //completion rule 3
  def completionRule3(uAxioms: RDD[(Int, Int)], 
      type3Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {
    val r3Join = type3Axioms.join(uAxioms)
    val r3Output = r3Join.map({ case (k, ((v1, v2), v3)) => (v1, (v3, v2)) })
                         .distinct
                         .partitionBy(type3Axioms.partitioner.get)
    r3Output

  }

  //completion rule 4
  def completionRule4(uAxioms: RDD[(Int, Int)], rAxioms: RDD[(Int, (Int, Int))], 
      type4Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, Int)] = {

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
    
    r4Join2Filtered
  }
  
   def completionRule4_new(uAxioms: RDD[(Int, Int)], 
       rAxioms: RDD[(Int, (Int, Int))], 
       type4Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, Int)] = {

    println("Debugging with persist(StorageLevel.MEMORY_ONLY_SER)")
    
    var t_begin = System.nanoTime()
    val r4Join1 = type4Axioms.join(rAxioms).map({ case (k, ((v1, v2), (v3, v4))) => (v4, (v2, (v3, v1))) })
    val r4Join1_count = r4Join1.persist(StorageLevel.MEMORY_ONLY_SER).count
    var t_end = System.nanoTime()
    println("type4Axioms.join(rAxioms). Count= " +r4Join1_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    //debug
    println("r4Join1: #Partitions = " + r4Join1.partitions.size + 
        " Size = " + SizeEstimator.estimate(r4Join1) + 
        " Count = " + r4Join1_count + 
        ", Time taken: " + (t_end - t_begin) / 1e6 + " ms")
    
    if(r4Join1.isEmpty()) return sc.emptyRDD
    
    val uAxiomsFlipped = uAxioms.map({ case (k1, v5) => (v5, k1) })
    
    t_begin = System.nanoTime()
    val r4Join2 = r4Join1.join(uAxiomsFlipped)
    val r4Join2_count = r4Join2.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("r4Join1ReMapped.join(uAxioms). Count= " + r4Join2_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    //debug
    println("r4Join1: #Partitions = " + r4Join2.partitions.size + 
        " Size = " + SizeEstimator.estimate(r4Join2) + 
        " Count = " + r4Join2_count + 
        ", Time taken: " + (t_end - t_begin) / 1e6 + " ms")
    
    t_begin = System.nanoTime()
    val r4Join2Filtered = r4Join2.filter({ case (k, ((v2, (v3, v1)), k1)) => v1 == k1 }).map({ case (k, ((v2, (v3, v1)), k1)) => (v2, v3) }).distinct
    val r4Join2Filtered_count = r4Join2Filtered.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("r4Join2.filter().map(). Count = " +r4Join2Filtered_count +", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    //debug
    println("r4Join2.filter().map(): #Partitions = " + r4Join2Filtered.partitions.size + 
        " Size = " + SizeEstimator.estimate(r4Join2Filtered) + 
        " Count = " + r4Join2Filtered_count + 
        ", Time taken: " + (t_end - t_begin) / 1e6 + " ms")
    
    r4Join2Filtered
  }
   
   def completionRule4_Raghava(filteredUAxioms: RDD[(Int, Int)], 
       rAxioms: RDD[(Int, (Int, Int))], 
       type4Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, Int)] = {

    println("Debugging with persist(StorageLevel.MEMORY_ONLY_SER)")  
    var t_begin = System.nanoTime()
    val type4AxiomsFillerKey = type4Axioms.map({ case (r, (a, b)) => (a, (r, b)) })
                                          .partitionBy(type4Axioms.partitioner.get)
    val r4Join1 = type4AxiomsFillerKey.join(filteredUAxioms) 
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
                          .partitionBy(type4Axioms.partitioner.get)
    val r4ResultCount = r4Result.persist(StorageLevel.MEMORY_ONLY_SER).count()
    t_end = System.nanoTime()
     println("r4ResultCount: #Partitions = " + r4Result.partitions.size + 
        " Size = " + SizeEstimator.estimate(r4Result) + 
        " Count=  " +r4ResultCount+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")    
     r4Result
   }

  //completion rule 5
  def completionRule5(rAxioms: RDD[(Int, (Int, Int))], type5Axioms: RDD[(Int, Int)]): RDD[(Int, (Int, Int))] = {

    val r5Join = type5Axioms.join(rAxioms)
                            .map({ case (k, (v1, (v2, v3))) => (v1, (v2, v3)) })
                            .distinct
                            .partitionBy(type5Axioms.partitioner.get)
   // val rAxiomsNew = rAxioms.union(r5Join).distinct

    r5Join
  }

  //completion rule 6
  def completionRule6(rAxioms: RDD[(Int, (Int, Int))], type6Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {

    var t_begin = System.nanoTime()
    val r6Join1 = type6Axioms.join(rAxioms).map({ case (k, ((v1, v2), (v3, v4))) => (v1, (v2, (v3, v4))) })
    val r6Join1_count = r6Join1.persist(StorageLevel.MEMORY_ONLY_SER).count
    var t_end = System.nanoTime()
    println("r6Join1=type6Axioms.join(rAxioms).map(). Count= " +r6Join1_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    t_begin = System.nanoTime()
    val r6Join2 = r6Join1.join(rAxioms)
    val r6Join2_count = r6Join2.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("r6Join2=r6Join1.join(rAxioms). Count= " +r6Join2_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    t_begin = System.nanoTime()
    val r6Join2_filtered = r6Join2.filter({ case (k, ((v2, (v3, v4)), (v5, v6))) => v4 == v5 })
                                  .map({ case (k, ((v2, (v3, v4)), (v5, v6))) => (v2, (v3, v6)) })
                                  .distinct
                                  .partitionBy(type6Axioms.partitioner.get)
    val r6Join2_filtered_count = r6Join2_filtered.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("r6Join2.filter().map(). Count= " +r6Join2_filtered_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    // val rAxiomsNew = rAxioms.union(r6Join2).distinct

    r6Join2_filtered
  }

  //completion rule 6
  def completionRule6_new(rAxioms: RDD[(Int, (Int, Int))], type6Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {

    var t_begin = System.nanoTime()
    val r6Join1 = type6Axioms.join(rAxioms).map({ case (k, ((v1, v2), (v3, v4))) => (v4, (v1, v2, v3)) })
    val r6Join1_count = r6Join1.persist(StorageLevel.MEMORY_ONLY_SER).count
    var t_end = System.nanoTime()
    println("r6Join1=type6Axioms.join(rAxioms).map(). Count= " +r6Join1_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    if(r6Join1.isEmpty()) return sc.emptyRDD
    
    val rAxiomsReMapped = rAxioms.map({ case (r,(y,z)) => (y,(r,z))})
    
    t_begin = System.nanoTime()
    val r6Join2 = r6Join1.join(rAxiomsReMapped)
    val r6Join2_count = r6Join2.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("r6Join2=r6Join1.join(rAxioms). Count= " +r6Join2_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    t_begin = System.nanoTime()
    val r6Join2_filtered = r6Join2.filter({ case (k, ((v1,v2,v3),(r,z))) => v1 == r })
                                  .map({ case (k, ((v1,v2,v3),(r,z))) => (v2, (v3, z)) })
                                  .distinct
                                  .partitionBy(type6Axioms.partitioner.get)
    val r6Join2_filtered_count = r6Join2_filtered.persist(StorageLevel.MEMORY_ONLY_SER).count
    t_end = System.nanoTime()
    println("r6Join2.filter().map(). Count= " +r6Join2_filtered_count+", Time taken: "+(t_end - t_begin) / 1e6 + " ms")
    
    // val rAxiomsNew = rAxioms.union(r6Join2).distinct

    r6Join2_filtered
  }

  
  //Computes time of any function passed to it
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1e6 + " ms")
    result
  }
  
  def deleteDir(dirPath: String): Boolean = {
    val localFileScheme = "file:///"
    val hdfsFileScheme = "hdfs://"
    val fileSystemURI: URI = {
      if(dirPath.startsWith(localFileScheme))
        new URI(localFileScheme)
      else if(dirPath.startsWith(hdfsFileScheme))
        new URI(hdfsFileScheme + "/")
      else 
        null
    }
    // delete the output directory
    val hadoopConf = new Configuration()
    require(fileSystemURI != null, "Provide file:/// or hdfs:// for " + 
            "input/output directories")
    val fileSystem = FileSystem.get(fileSystemURI, hadoopConf)
    fileSystem.delete(new Path(dirPath), true)
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

    conf.registerKryoClasses(Array(classOf[scala.collection.immutable.Set[Int]]))
    deleteDir(args(1))
    
    val numProcessors = Runtime.getRuntime.availableProcessors()
    numPartitions = numProcessors * args(2).toInt

    var (uAxioms, rAxioms, type1Axioms, type2Axioms, type3Axioms, type4Axioms, 
        type5Axioms, type6Axioms) = initializeRDD(sc, args(0))
    uAxioms = uAxioms.cache()
   
   
    println("Before closure computation. Initial uAxioms count: " + 
        uAxioms.count + ", Initial rAxioms count: " + rAxioms.count)
    
    //used for filtering of uaxioms in rule4
    val type4Fillers = type4Axioms.collect().map({ case (k, (v1, v2)) => v1 }).toSet
    val type4FillersBroadcast = sc.broadcast(type4Fillers)
    
    var currUAllRules = uAxioms
    var currRAllRules = rAxioms
    
    var currUAllRulesCount = uAxioms.count
    var currRAllRulesCount = rAxioms.count
    
    var prevUAllRulesCount: Long = 0
    var prevRAllRulesCount: Long = 0 
    var prevDeltaURule1: RDD[(Int, Int)] = null
    var currDeltaURule1: RDD[(Int, Int)] = null
    var prevDeltaURule2: RDD[(Int, Int)] = null 
    var currDeltaURule2: RDD[(Int, Int)] = null
    var prevDeltaRRule3: RDD[(Int, (Int, Int))] = null 
    var currDeltaRRule3: RDD[(Int, (Int, Int))] = null
    var prevDeltaURule4: RDD[(Int, Int)] = null 
    var currDeltaURule4: RDD[(Int, Int)] = null
    var prevDeltaRRule5: RDD[(Int, (Int, Int))] = null 
    var currDeltaRRule5: RDD[(Int, (Int, Int))] = null
    var prevDeltaRRule6: RDD[(Int, (Int, Int))] = null 
    var currDeltaRRule6: RDD[(Int, (Int, Int))] = null
     
     var counter = 1
     
     while (prevUAllRulesCount != currUAllRulesCount || prevRAllRulesCount != currRAllRulesCount) {

       var t_beginLoop = System.nanoTime()

       val inputURule1: RDD[(Int, Int)] = { 
         if (counter == 1)
           currUAllRules
         else  
           sc.union(prevDeltaURule1, prevDeltaURule2, prevDeltaURule4) 
             .partitionBy(type1Axioms.partitioner.get)
         }
       currDeltaURule1 = completionRule1(inputURule1, type1Axioms) //Rule1
       println("----Completed rule1----")
     
       val inputURule2 = sc.union(currUAllRules, 
           currDeltaURule1).distinct.partitionBy(type2Axioms.partitioner.get)
       currDeltaURule2 = completionRule2(inputURule2, type2Axioms) //Rule2
       println("----Completed rule2----")
      
      
      val inputURule3 = { 
         if (counter == 1)
           sc.union(inputURule2, currDeltaURule2)
             .partitionBy(type3Axioms.partitioner.get)
         else
           sc.union(prevDeltaURule4, currDeltaURule1, currDeltaURule2) 
             .partitionBy(type3Axioms.partitioner.get)
         }
      currDeltaRRule3 = completionRule3(inputURule3, type3Axioms) //Rule3
      println("----Completed rule3----")      

      // caching this rdd since it gets reused in rule5 and rule6
      val inputRRule4 = sc.union(currRAllRules, currDeltaRRule3)
                          .distinct
                          .partitionBy(type4Axioms.partitioner.get)
                          .persist()
      val inputURule4 = { 
        if (counter == 1)
          inputURule3
        else 
          sc.union(inputURule2, currDeltaURule2)
            .distinct
            .partitionBy(type4Axioms.partitioner.get)
        }
      
      //raghava's new rule 4
      val filteredUAxiomsRule2 = inputURule4.filter({ 
          case (k, v) => type4FillersBroadcast.value.contains(k) })
                                  .partitionBy(type4Axioms.partitioner.get)      
      currDeltaURule4 = completionRule4_Raghava(filteredUAxiomsRule2, 
          inputRRule4, type4Axioms)
      println("----Completed rule4----")

      val inputRRule5 = inputRRule4 //no change in R after rule 4
      currDeltaRRule5 = completionRule5(inputRRule5, type5Axioms) //Rule5      
      println("----Completed rule5----")

      // var inputRRule6 = sc.union(prevDeltaRRule6, currDeltaRRule3, currDeltaRRule5)      
      //debugging
      val inputRRule6 = sc.union(inputRRule4, currDeltaRRule5)
                          .distinct
                          .partitionBy(type6Axioms.partitioner.get)
      currDeltaRRule6 = completionRule6_new(inputRRule6, type6Axioms) //Rule6      
      println("----Completed rule6----")

      //repartition U and R axioms   
//      currDeltaURule1 = currDeltaURule1.repartition(numProcessors).cache()
//      currDeltaURule2 = currDeltaURule2.repartition(numProcessors).cache()
//      currDeltaRRule3 = currDeltaRRule3.repartition(numProcessors).cache()
//      currDeltaURule4 = currDeltaURule4.repartition(numProcessors).cache()
//      currDeltaRRule5 = currDeltaRRule5.repartition(numProcessors).cache()
//      currDeltaRRule6 = currDeltaRRule6.repartition(numProcessors).cache()

      prevUAllRulesCount = currUAllRulesCount
      prevRAllRulesCount = currRAllRulesCount
      
      //store the union of all new axioms 
      var t_begin_uAxiomCount = System.nanoTime() 
      currUAllRules = sc.union(currUAllRules, currDeltaURule1, currDeltaURule2, 
          currDeltaURule4)
                        .distinct.partitionBy(type1Axioms.partitioner.get)
                        .persist()
      currUAllRulesCount = currUAllRules.count
      var t_end_uAxiomCount = System.nanoTime()
      println("Time taken for uAxiom count: "+ (t_begin_uAxiomCount - t_end_uAxiomCount) / 1e6 + " ms")
      println("------Completed uAxioms count--------")
        
      var t_begin_rAxiomCount = System.nanoTime()
      currRAllRules = sc.union(currRAllRules, currDeltaRRule3, currDeltaRRule5, 
          currDeltaRRule6)
                        .distinct.partitionBy(type1Axioms.partitioner.get)
                        .persist()
      currRAllRulesCount = currRAllRules.count       
      var t_end_rAxiomCount = System.nanoTime()
      println("Time taken for rAxiom count: "+ (t_end_rAxiomCount - t_begin_rAxiomCount) / 1e6 + " ms")
      println("------Completed rAxioms count--------")
       
      //time
      var t_endLoop = System.nanoTime()
      
      //debugging
      println("===================================debug info=========================================")
      println("End of loop: " + counter + ". New uAxioms count: " + 
          currUAllRulesCount + ", New rAxioms count: " + currRAllRulesCount)
      println("Runtime of the current loop: " + (t_endLoop - t_beginLoop) / 1e6 + " ms")
      println("======================================================================================")
      
      prevDeltaURule1 = currDeltaURule1
      prevDeltaURule2 = currDeltaURule2
      prevDeltaRRule3 = currDeltaRRule3
      prevDeltaURule4 = currDeltaURule4
      prevDeltaRRule5 = currDeltaRRule5
      prevDeltaRRule6 = currDeltaRRule6
      
      //loop counter 
      counter = counter + 1

    } //end of loop

    println("Closure computed. Final number of uAxioms: " + currUAllRulesCount)
    
    val t_end = System.nanoTime()

    //collect result (new subclass axioms) into 1 partition and spit out the result to a file.
    val sAxioms = currUAllRules.map({ case (v1, v2) => v2 + "|" + v1 }) // invert uAxioms to sAxioms
    sAxioms.coalesce(1, true).saveAsTextFile(args(1)) // coalesce to 1 partition so output can be written to 1 file
    println("Total runtime of the program: " + (t_end - t_init) / 1e6 + " ms")
    sc.stop()
  }
  
}