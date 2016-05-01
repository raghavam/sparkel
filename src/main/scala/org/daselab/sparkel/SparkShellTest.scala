package org.daselab.sparkel

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.rdd._
import java.io.File
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.apache.spark.HashPartitioner
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI
import org.apache.spark.broadcast.Broadcast

object SparkShellTest {

  var numPartitions = -1 // later initialized from command line
  var hashPartitioner: HashPartitioner = null

  /*
   * Initializes all the RDDs corresponding to each axiom-type. 
   * Note, we want all the typeAxioms to reside in cache() and not on disk, 
   * hence we are using the default persistence level, i.e. Memory_Only.
   */
  def initializeRDD(sc: SparkContext, dirPath: String) = {

    dummyStage(sc, dirPath)    
    val uAxioms = sc.textFile(dirPath + "sAxioms.txt")
                    .map[(Int, Int)](line => { line.split("\\|") match { case Array(x, a) => (a.toInt, x.toInt) }})
                    .partitionBy(hashPartitioner)
                    .setName("uAxioms")
                    .persist(StorageLevel.MEMORY_AND_DISK)
      
    uAxioms.count()

    val rAxioms: RDD[(Int, (Int, Int))] = sc.emptyRDD

    val type1Axioms = sc.textFile(dirPath + "Type1Axioms.txt")
                        .map[(Int, Int)](line => {line.split("\\|") match { case Array(a, b) => (a.toInt, b.toInt)}})
                        .partitionBy(hashPartitioner)
                        .setName("type1Axioms")
                        .persist(StorageLevel.MEMORY_AND_DISK)
   
     type1Axioms.count()
      
     val uAxiomsFlipped = uAxioms.map({ case (a, x) => (x, a) })
                                 .partitionBy(hashPartitioner)
                                 .setName("uAxiomsFlipped")
                                 .persist(StorageLevel.MEMORY_AND_DISK)
                                 
     uAxiomsFlipped.count()
    
      
    val type2Axioms = sc.textFile(dirPath + "Type2Axioms.txt")
                        .map[((Int, Int), Int)](line => {line.split("\\|") match {
                            case Array(a1, a2, b) => ((a1.toInt, a2.toInt), b.toInt)
                              }
                         })
                        .partitionBy(hashPartitioner)
                        .setName("type2Axioms")
                        .persist(StorageLevel.MEMORY_AND_DISK)
                        
    type2Axioms.count()     
 
    val type2AxiomsConjunctsFlipped = type2Axioms.map({ case ((a1, a2), b) => ((a2, a1), b) })
                                     .partitionBy(hashPartitioner)
                                     .setName("type2AxiomsMap2")
                                     .persist(StorageLevel.MEMORY_AND_DISK)
                                     
    type2AxiomsConjunctsFlipped.count()

    val type3Axioms = sc.textFile(dirPath + "Type3Axioms.txt")
                        .map[(Int, (Int, Int))](line => {
                            line.split("\\|") match {
                            case Array(x, y, z) => (x.toInt, (y.toInt, z.toInt))
                            }
                         })
                        .partitionBy(hashPartitioner)
                        .setName("type3Axioms")
                        .persist(StorageLevel.MEMORY_AND_DISK)
                        
    type3Axioms.count()
      
    val type4Axioms = sc.textFile(dirPath + "Type4Axioms.txt")
                        .map[(Int, (Int, Int))](line => {
                            line.split("\\|") match {
                            case Array(x, y, z) => (x.toInt, (y.toInt, z.toInt))
                            }
                         })
                        .partitionBy(hashPartitioner)
                        .setName("type4Axioms")
                        .persist(StorageLevel.MEMORY_AND_DISK)
      
    val type5Axioms = sc.textFile(dirPath + "Type5Axioms.txt")
                        .map[(Int, Int)](line => {
                            line.split("\\|") match {
                            case Array(x, y) => (x.toInt, y.toInt)
                            }
                         })
                        .partitionBy(hashPartitioner)
                        .setName("type5Axioms")
                        .persist(StorageLevel.MEMORY_AND_DISK)
    type5Axioms.count()
      
    val type6Axioms = sc.textFile(dirPath + "Type6Axioms.txt")
                        .map[(Int, (Int, Int))](line => {
                            line.split("\\|") match {
                            case Array(x, y, z) => (x.toInt, (y.toInt, z.toInt))
                            }
                         })
                        .partitionBy(hashPartitioner)
                        .setName("type6Axioms")
                        .persist(StorageLevel.MEMORY_AND_DISK)
                        
     type6Axioms.count()                   

    //return the initialized RDDs as a Tuple object (can have at max 22 elements in Spark Tuple)
    (uAxioms, uAxiomsFlipped, rAxioms, type1Axioms, type2Axioms, type2AxiomsConjunctsFlipped, 
        type3Axioms, type4Axioms, type5Axioms, type6Axioms)
  }
  
  /**
   * Introduced a dummy stage to compensate for initial scheduling delay of 
   * the executors. This solves the issue of all-nothing data skew, i.e., all 
   * the partitions of an RDD are assigned to just one node. 
   */
  def dummyStage(sc: SparkContext, dirPath: String): Unit = {
    val sAxioms = sc.textFile(dirPath + "sAxioms.txt").map[(Int, Int)](line => { 
                                                          line.split("\\|") match { 
                                                          case Array(x, y) => (x.toInt, y.toInt) 
                                                          }})
                                                      .partitionBy(hashPartitioner)
                                                      .setName("sAxioms")
                                                      
      
     sAxioms.persist().count()
     sAxioms.unpersist().count()
  }
  
   //completion rule1
  def completionRule1(deltaUAxioms: RDD[(Int, Int)], type1Axioms: RDD[(Int, Int)], 
      loopCounter: Int): RDD[(Int, Int)] = {

    val r1Join = type1Axioms.join(deltaUAxioms)
                            .values
//                            .distinct(numPartitions)
                            .partitionBy(hashPartitioner)
                              
    r1Join
  }
  

  def completionRule2(loopCounter: Int, type2A1A2: Broadcast[Set[(Int, Int)]], 
      deltaUAxiomsFlipped: RDD[(Int, Int)], uAxiomsFlipped: RDD[(Int, Int)], type2Axioms: RDD[((Int, Int), Int)], 
      type2AxiomsConjunctsFlipped: RDD[((Int, Int), Int)]): RDD[(Int, Int)] = {
        
    //JOIN 1
    val r2Join1 = uAxiomsFlipped.join(deltaUAxiomsFlipped)
                                .setName("r2Join1_" + loopCounter)

    //filter joined uaxioms result before remapping for second join
    val r2JoinFilter = r2Join1.filter{ case (x, (a1, a2)) => type2A1A2.value.contains((a1, a2)) || type2A1A2.value.contains((a2, a1)) } //need the flipped combination for delta
                              .setName("r2JoinFilter_" + loopCounter) 
    //JOIN 2 - PART 1
    val r2JoinFilterMap = r2JoinFilter.map({ case (x, (a1, a2)) => ((a1, a2), x) })
                                      .partitionBy(hashPartitioner)
                                      .setName("r2JoinFilterMap_" + loopCounter)                                     
    val r2Join21 = r2JoinFilterMap.join(type2Axioms)
                                   .map({ case ((a1, a2), (x, b)) => (b, x) })
                                  .partitionBy(hashPartitioner)
                                  .setName("r2Join21_" + loopCounter)

    //JOIN 2 - PART 2
    val r2Join22 = r2JoinFilterMap.join(type2AxiomsConjunctsFlipped)
                                  .map({ case ((a1, a2), (x, b)) => (b, x) })
                                  .partitionBy(hashPartitioner)
                                  .setName("r2Join22_" + loopCounter)

    //UNION join results
    var r2Join2 = r2Join21.union(r2Join22).setName("r2Join2_" + loopCounter)

    r2Join2
  }
  
   //completion rule 3
  def completionRule3(deltUAxioms: RDD[(Int, Int)], type3Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {
    val r3Join = type3Axioms.join(deltUAxioms)
    val r3Output = r3Join.map({ case (k, ((v1, v2), v3)) => (v1, (v3, v2)) })
                         .partitionBy(hashPartitioner)
                         
    r3Output

  }
  
  def completionRule4(filteredUAxioms: RDD[(Int, Int)], 
       rAxioms: RDD[(Int, (Int, Int))], 
       type4Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, Int)] = { 
    val type4AxiomsFillerKey = type4Axioms.map({ case (r, (a, b)) => (a, (r, b)) })
                                          .partitionBy(hashPartitioner)
    val r4Join1 = type4AxiomsFillerKey.join(filteredUAxioms) 
           
    val r4Join1YKey = r4Join1.map({ case (a, ((r1, b), y)) => (y, (r1, b)) })
                             .partitionBy(hashPartitioner)
    val rAxiomsPairYKey = rAxioms.map({ case (r2, (x, y)) => (y, (r2, x)) })
                                 .partitionBy(hashPartitioner)
    val r4Join2 = r4Join1YKey.join(rAxiomsPairYKey)

    val r4Result = r4Join2.filter({ case (y, ((r1, b), (r2, x))) => r1 == r2 })
                          .map({ case (y, ((r1, b), (r2, x))) => (b, x) })
                          .partitionBy(hashPartitioner)
   
     r4Result
   }
  
  def completionRule4_v2(filteredUAxioms: RDD[(Int, Int)], 
       rAxioms: RDD[(Int, (Int, Int))], 
       type4Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, Int)] = { 
    val type4AxiomsFillerKey = type4Axioms.map({ case (r, (a, b)) => (a, (r, b)) })
                                          .partitionBy(hashPartitioner)
    val r4Join1 = type4AxiomsFillerKey.join(filteredUAxioms) 
           
    val r4Join1YKey = r4Join1.map({ case (a, ((r1, b), y)) => ((r1, y), b) })
                             .partitionBy(hashPartitioner)
    val rAxiomsPairYKey = rAxioms.map({ case (r2, (x, y)) => ((r2, y), x) })
                                 .partitionBy(hashPartitioner)
    val r4Join2 = r4Join1YKey.join(rAxiomsPairYKey)

    val r4Result = r4Join2.map({ case ((r, y), (b, x)) => (b, x) })
                          .partitionBy(hashPartitioner)
   
    r4Result
   }
  
  def completionRule4_delta(filteredUAxioms: RDD[(Int, Int)], 
       rAxioms: RDD[(Int, (Int, Int))], 
       type4Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, Int)] = { 
    val type4AxiomsFillerKey = type4Axioms.map({ case (r, (a, b)) => (a, (r, b)) })
                                          .partitionBy(hashPartitioner)
    val r4Join1 = type4AxiomsFillerKey.join(filteredUAxioms) 
           
    val r4Join1YKey = r4Join1.map({ case (a, ((r1, b), y)) => ((r1, y), b) })
                             .partitionBy(hashPartitioner)
    val rAxiomsPairYKey = rAxioms.map({ case (r2, (x, y)) => ((r2, y), x) })
                                 .partitionBy(hashPartitioner)
    val r4Join2 = r4Join1YKey.join(rAxiomsPairYKey)

    val r4Result = r4Join2.map({ case ((r, y), (b, x)) => (b, x) })
                          .partitionBy(hashPartitioner)
   
    r4Result
   }
  
  //completion rule 5
  def completionRule5(deltaRAxioms: RDD[(Int, (Int, Int))], 
      type5Axioms: RDD[(Int, Int)]): RDD[(Int, (Int, Int))] = {
    val r5Join = type5Axioms.join(deltaRAxioms)
                            .map({ case (k, (v1, (v2, v3))) => (v1, (v2, v3)) })
                            .partitionBy(hashPartitioner)

    r5Join
  }
  
  
  //completion rule 6
  def completionRule6_new(rAxioms: RDD[(Int, (Int, Int))], 
      type6Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {
   
    val r6Join1 = type6Axioms.join(rAxioms)
                             .map({ case (r, ((s, t), (x, y))) => (y, (s, t, x)) })
                             .partitionBy(hashPartitioner)

    
    val rAxiomsReMapped = rAxioms.map({ case (r, (y, z)) => (y, (r, z))})
                                 .partitionBy(hashPartitioner)
    
   
    val r6Join2 = r6Join1.join(rAxiomsReMapped)
                        // .partitionBy(hashPartitioner)


    val r6Join2Filtered = r6Join2.filter({ case (y, ((s, t, x), (r, z))) => s == r })
                                 .map({ case (y, ((s, t, x), (r, z))) => (t, (x, z)) })
                                 .partitionBy(hashPartitioner)
   
    r6Join2Filtered
  }
  
  
  /**
   * For a hash partitioned RDD, it is sufficient to check for duplicate 
   * entries within a partition instead of checking them across the cluster. 
   * This avoids a shuffle operation. 
   */
  def customizedDistinctForUAxioms(rdd: RDD[(Int, Int)]): RDD[(Int, Int)] = {    
    val uAxiomsDeDup = rdd.mapPartitions ({
                        iterator => {
                           val axiomsSet = iterator.toSet
                           axiomsSet.iterator
                        }
                     }, true)                     
    uAxiomsDeDup                    
  }
  
  /**
   * similar to {@link #customizedDistinct(rdd: RDD[(Int, Int)])}
   */
  def customizedDistinctForRAxioms(rdd: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {    
    val rAxiomsDeDup = rdd.mapPartitions ({
                        iterator => {
                           val axiomsSet = iterator.toSet
                           axiomsSet.iterator
                        }
                     }, true)                     
    rAxiomsDeDup                    
  }

  //Deletes the existing output directory
  def deleteDir(dirPath: String): Boolean = {
    val localFileScheme = "file:///"
    val hdfsFileScheme = "hdfs://"
    val fileSystemURI: URI = {
      if (dirPath.startsWith(localFileScheme))
        new URI(localFileScheme)
      else if (dirPath.startsWith(hdfsFileScheme))
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

  def prepareRule1Inputs(loopCounter: Int, sc: SparkContext, 
      uAxiomsFinal: RDD[(Int, Int)], prevDeltaURule1: RDD[(Int, Int)], 
      prevDeltaURule2: RDD[(Int, Int)], 
      prevDeltaURule4: RDD[(Int, Int)]): RDD[(Int, Int)] = {
    if (loopCounter == 1)
      uAxiomsFinal
    else
//      sc.union(prevDeltaURule1, prevDeltaURule2, prevDeltaURule4)
      sc.union(prevDeltaURule1, prevDeltaURule2)
        .partitionBy(hashPartitioner) 
        .setName("deltaUAxiomsForRule2_" + loopCounter)
  }
  
  def prepareRule2Inputs(loopCounter: Int, sc: SparkContext, 
      uAxiomsFinal: RDD[(Int, Int)], currDeltaURule1: RDD[(Int, Int)], 
      prevDeltaURule2: RDD[(Int, Int)], prevDeltaURule4: RDD[(Int, Int)], 
      uAxiomsFlipped: RDD[(Int, Int)]) = {
    var uAxiomsRule1 = uAxiomsFinal.union(currDeltaURule1)
    uAxiomsRule1 = customizedDistinctForUAxioms(uAxiomsRule1)
                          .setName("uAxiomsRule1_" + loopCounter)
     
    val deltaUAxiomsForRule2 = {
      if (loopCounter == 1)
        uAxiomsRule1 //uAxiom total for first loop
      else
//        sc.union(currDeltaURule1, prevDeltaURule2, prevDeltaURule4)
        sc.union(currDeltaURule1, prevDeltaURule2)
          .partitionBy(hashPartitioner) 
          .setName("deltaUAxiomsForRule2_" + loopCounter)
    }
    //flip delta uAxioms
    val deltaUAxiomsFlipped = deltaUAxiomsForRule2.map({ case (a, x) => (x, a) }) 
                                                  .partitionBy(hashPartitioner)
                                                  .setName("deltaUAxiomsFlipped_" + loopCounter)
    //update uAxiomsFlipped
    var uAxiomsFlippedNew = sc.union(uAxiomsFlipped, deltaUAxiomsFlipped)
                              .partitionBy(hashPartitioner)
    uAxiomsFlippedNew = customizedDistinctForUAxioms(uAxiomsFlippedNew)
                              .setName("uAxiomsFlipped_" + loopCounter)
    (uAxiomsRule1, deltaUAxiomsForRule2, deltaUAxiomsFlipped, uAxiomsFlippedNew)                          
  }
  
  def prepareRule3Inputs(loopCounter: Int, sc: SparkContext, 
      uAxiomsRule1: RDD[(Int, Int)], currDeltaURule1: RDD[(Int, Int)], 
      currDeltaURule2: RDD[(Int, Int)], prevDeltaURule4: RDD[(Int, Int)]) = {
    var uAxiomsRule2 = uAxiomsRule1.union(currDeltaURule2)
    uAxiomsRule2 = customizedDistinctForUAxioms(uAxiomsRule2)
                              .setName("uAxiomsRule2_" + loopCounter)                             
    val deltaUAxiomsForRule3 = { 
       if (loopCounter == 1)
         uAxiomsRule2
       else
//         sc.union(currDeltaURule1, currDeltaURule2, prevDeltaURule4)
         sc.union(currDeltaURule1, currDeltaURule2)
           .partitionBy(hashPartitioner) 
           .setName("deltaUAxiomsForRule3_" + loopCounter)
       }
    (uAxiomsRule2, deltaUAxiomsForRule3)
  }
  
  def prepareRule4Inputs(loopCounter: Int, currDeltaRRule3: RDD[(Int, (Int, Int))], 
      rAxiomsFinal: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {
    var rAxiomsRule3 = {
      if(loopCounter == 1)
        currDeltaRRule3
      else
        rAxiomsFinal.union(currDeltaRRule3) //partitionAware union
      } 
    rAxiomsRule3 = customizedDistinctForRAxioms(rAxiomsRule3)
                                  .setName("rAxiomsRule3_" + loopCounter)
    rAxiomsRule3                              
  }
  
  def prepareRule5Inputs(loopCounter: Int, sc: SparkContext, 
      rAxiomsRule3: RDD[(Int, (Int, Int))], prevDeltaRRule5: RDD[(Int, (Int, Int))], 
      currDeltaRRule3: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {
    val deltaRAxiomsToRule5 = { 
      if (loopCounter == 1)
        rAxiomsRule3
      else
        sc.union(prevDeltaRRule5, currDeltaRRule3)
          .partitionBy(hashPartitioner)
      }
    deltaRAxiomsToRule5
  }
  
  /*
   * The main method that inititalizes and calls each function corresponding to the completion rule 
   */
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.println("Missing args:\n\t 0. input directory containing the axiom files \n\t" +
        "1. output directory to save the final computed sAxioms \n\t 2. Number of worker nodes in the cluster \n\t" +
        "3. Number of partitions (initial)")
      System.exit(-1)
    }

    val dirDeleted = deleteDir(args(1))
    numPartitions = args(3).toInt
    hashPartitioner = new HashPartitioner(numPartitions)
    val dirPath = args(0)
    
    //init time
    val t_init = System.nanoTime()

    val conf = new SparkConf().setAppName("SparkEL")
    val sc = new SparkContext(conf)

    var (uAxioms, uAxiomsFlipped, rAxioms, type1Axioms, type2Axioms, type2AxiomsConjunctsFlipped, 
        type3Axioms, type4Axioms, type5Axioms, type6Axioms) = initializeRDD(sc, dirPath)
      
     //  Thread.sleep(30000) //sleep for a minute  

    println("Before closure computation. Initial uAxioms count: " + uAxioms.count + 
                                      ", Initial rAxioms count: " + rAxioms.count)

    
    var loopCounter: Int = 0

    var uAxiomsFinal = uAxioms
    var rAxiomsFinal = rAxioms

    var currDeltaURule1: RDD[(Int, Int)] = sc.emptyRDD
    var currDeltaURule2: RDD[(Int, Int)] = sc.emptyRDD
    var currDeltaURule4: RDD[(Int, Int)] = sc.emptyRDD
    
    var prevDeltaURule1: RDD[(Int, Int)] = sc.emptyRDD
    var prevDeltaURule2: RDD[(Int, Int)] = sc.emptyRDD
    var prevDeltaRRule3: RDD[(Int, (Int, Int))] = sc.emptyRDD
    var prevDeltaURule4: RDD[(Int, Int)] = sc.emptyRDD
    var prevDeltaRRule5: RDD[(Int, (Int, Int))] = sc.emptyRDD
    var prevDeltaRRule6: RDD[(Int, (Int, Int))] = sc.emptyRDD
    var prevUAxiomsFlipped = uAxiomsFlipped
    var prevUAxiomsFinal= uAxioms
    var prevRAxiomsFinal = rAxioms
    

    //for pre-filtering for rule2
    val type2Collect = type2Axioms.collect()
    val type2FillersA1A2 = type2Collect.map({ case ((a1, a2), b) => (a1, a2) }).toSet
    val type2ConjunctsBroadcast = sc.broadcast(type2FillersA1A2)
    
    // used for filtering of uaxioms in rule4
    val type4Fillers = type4Axioms.collect().map({ case (k, (v1, v2)) => v1 }).toSet
    val type4FillersBroadcast = { 
      if (!type4Fillers.isEmpty)
        sc.broadcast(type4Fillers) 
      else 
        null
      }

    while (loopCounter <= 25) {

      loopCounter += 1

      //Rule 1
      val deltaUAxiomsForRule1 = prepareRule1Inputs(loopCounter, sc, uAxiomsFinal, 
                                                     prevDeltaURule1, prevDeltaURule2, 
                                                     prevDeltaURule4) 
      var currDeltaURule1 = completionRule1(deltaUAxiomsForRule1, type1Axioms, loopCounter)
      println("----Completed rule1----")
      println("=====================================")
      
      //Rule2          
      val (uAxiomsRule1, deltaUAxiomsForRule2, deltaUAxiomsFlipped, 
          uAxiomsFlippedNew) = prepareRule2Inputs(
                                    loopCounter, sc, 
                                    uAxiomsFinal, currDeltaURule1, 
                                    prevDeltaURule2, prevDeltaURule4, 
                                    uAxiomsFlipped)
      uAxiomsFlipped = uAxiomsFlippedNew                   
      var currDeltaURule2 = completionRule2(loopCounter, type2ConjunctsBroadcast, 
          deltaUAxiomsForRule2, uAxiomsFlipped, type2Axioms, type2AxiomsConjunctsFlipped)
      println("----Completed rule2----")
      println("=====================================")
      
      //Rule3
      var (uAxiomsRule2, deltaUAxiomsForRule3) = prepareRule3Inputs(loopCounter, 
          sc, uAxiomsRule1, currDeltaURule1, currDeltaURule2, prevDeltaURule4)
      var currDeltaRRule3 = completionRule3(deltaUAxiomsForRule3, type3Axioms) 
      println("----Completed rule3----")
      
      //Rule4
      var rAxiomsRule3 = prepareRule4Inputs(loopCounter, currDeltaRRule3, rAxiomsFinal)                         
      val filteredUAxiomsRule2 = { 
        if (type4FillersBroadcast != null)
          uAxiomsRule2.filter({ 
              case (k, v) => type4FillersBroadcast.value.contains(k) })
                     .partitionBy(hashPartitioner) 
        else
          sc.emptyRDD[(Int, Int)]
        }  
      currDeltaURule4 = completionRule4_v2(filteredUAxiomsRule2, 
          rAxiomsRule3, type4Axioms) 
      println("----Completed rule4----")     
      
      var uAxiomsRule4 = uAxiomsRule2.union(currDeltaURule4)
      uAxiomsRule4 = customizedDistinctForUAxioms(uAxiomsRule4)
                                     .setName("uAxiomsRule4_" + loopCounter)
      //get delta U for only the current iteration                               
      currDeltaURule4 = uAxiomsRule4.subtractByKey(uAxiomsRule2, hashPartitioner)                               
      
      //Rule 5 
      val deltaRAxiomsToRule5 = prepareRule5Inputs(loopCounter, sc, rAxiomsRule3, 
                                                 prevDeltaRRule5, currDeltaRRule3)
      var currDeltaRRule5 = completionRule5(deltaRAxiomsToRule5, type5Axioms) //Rule5  
      println("----Completed rule5----")
                   
      var rAxiomsRule5 = rAxiomsRule3.union(currDeltaRRule5)
      rAxiomsRule5 = customizedDistinctForRAxioms(rAxiomsRule5).setName("rAxiomsRule5_" + loopCounter) 
       
//      var currDeltaRRule6 = completionRule6_new(rAxiomsRule5, type6Axioms) 
//      println("----Completed rule6----")
       
//      var rAxiomsRule6 = rAxiomsRule5.union(currDeltaRRule6)
//      rAxiomsRule6 = customizedDistinctForRAxioms(rAxiomsRule6).setName("rAxiomsRule6_"+loopCounter)
       
       
      //TODO: update final vars to the last rule's vars for next iteration 
      uAxiomsFinal = uAxiomsRule4
      rAxiomsFinal = rAxiomsRule5
      
      uAxiomsFinal = uAxiomsFinal.setName("uAxiomsFinal_" + loopCounter)
                                 .persist(StorageLevel.MEMORY_AND_DISK)
                                 
      rAxiomsFinal = rAxiomsFinal.setName("rAxiomsFinal_"+loopCounter)
                                 .persist(StorageLevel.MEMORY_AND_DISK)
                                 
      
      var t_begin_uAxiomCount = System.nanoTime()
      val currUAxiomsCount = uAxiomsFinal.count()
      var t_end_uAxiomCount = System.nanoTime()
      println("------Completed uAxioms count at the end of the loop: " + loopCounter + "--------")
      println("uAxiomCount: " + currUAxiomsCount + ", Time taken for uAxiom count: " + 
          (t_end_uAxiomCount - t_begin_uAxiomCount) / 1e9 + " s")
      println("====================================")
      
      
      var t_begin_rAxiomCount = System.nanoTime()
      val currRAxiomsCount = rAxiomsFinal.count()
      var t_end_rAxiomCount = System.nanoTime()
      println("------Completed rAxioms count at the end of the loop: " + loopCounter + "--------")
      println("rAxiomCount: " + currRAxiomsCount + ", Time taken for uAxiom count: " + 
          (t_end_rAxiomCount - t_begin_rAxiomCount) / 1e9 + " s")
      println("====================================")
      
      
      
          
      //delta RDDs
      currDeltaURule1 = currDeltaURule1.setName("currDeltaURule1_" + loopCounter)
                                       .persist(StorageLevel.MEMORY_AND_DISK)
      currDeltaURule1.count()                               
                                       
      currDeltaURule2 = currDeltaURule2.setName("currDeltaURule2_" + loopCounter)
                                       .persist(StorageLevel.MEMORY_AND_DISK)
      
      currDeltaURule2.count()
      
      uAxiomsFlipped = uAxiomsFlipped.setName("uAxiomsFlipped_" + loopCounter)
                                     .persist(StorageLevel.MEMORY_AND_DISK)
      uAxiomsFlipped.count() 
      
      currDeltaRRule3 = currDeltaRRule3.setName("currDeltaRRule3_"+loopCounter)
                                       .persist(StorageLevel.MEMORY_AND_DISK)
      
      currDeltaRRule3.count()
      
      var timeDeltaUR4Begin = System.nanoTime()
      currDeltaURule4 = currDeltaURule4.setName("currDeltaURule4_" + loopCounter)
                                       .persist(StorageLevel.MEMORY_AND_DISK)
      
      currDeltaURule4.count()
      var timeDeltaUR4End = System.nanoTime()
      println("Time taken for currDeltaURule4 in loop " + loopCounter + ": " + 
          (timeDeltaUR4End - timeDeltaUR4Begin)/ 1e9 + " s")
      
      currDeltaRRule5 = currDeltaRRule5.setName("currDeltaRRule5_"+loopCounter)
                                       .persist(StorageLevel.MEMORY_AND_DISK)
                                       
      currDeltaRRule5.count()
      
//      currDeltaRRule6 = currDeltaRRule6.setName("currDeltaRRule6_"+loopCounter)
//                                       .persist(StorageLevel.MEMORY_AND_DISK)
                                       
//      currDeltaRRule6.count()
      
      //prev delta RDDs assignments
      prevUAxiomsFinal.unpersist()
      prevUAxiomsFinal = uAxiomsFinal
      prevRAxiomsFinal.unpersist()
      prevRAxiomsFinal = rAxiomsFinal
      prevDeltaURule1.unpersist()
      prevDeltaURule1 = currDeltaURule1
      prevDeltaURule2.unpersist()                                      
      prevDeltaURule2 = currDeltaURule2
      prevUAxiomsFlipped.unpersist()
      prevUAxiomsFlipped = uAxiomsFlipped
      prevDeltaRRule3.unpersist()
      prevDeltaRRule3 = currDeltaRRule3
      prevDeltaURule4.unpersist()                                      
      prevDeltaURule4 = currDeltaURule4
      prevDeltaRRule5.unpersist()
      prevDeltaRRule5 = currDeltaRRule5
      prevDeltaRRule6.unpersist()
//      prevDeltaRRule6 = currDeltaRRule6

    }
   
     val t_end = System.nanoTime()
     println("Total time taken for the program: "+ (t_end - t_init)/ 1e9 + " s")
     
     
     Thread.sleep(3000000) // add 100s delay for UI vizualization

    sc.stop()

  }

}