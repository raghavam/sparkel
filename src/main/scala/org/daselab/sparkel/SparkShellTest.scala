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
                            case Array(a, r, b) => (a.toInt, (r.toInt, b.toInt))
                            }
                         })
                        .partitionBy(hashPartitioner)
                        .setName("type3Axioms")
                        .persist(StorageLevel.MEMORY_AND_DISK)
                        
    type3Axioms.count()
      
    val type4Axioms = sc.textFile(dirPath + "Type4Axioms.txt")
                        .map[(Int, (Int, Int))](line => {
                            line.split("\\|") match {
                            case Array(r, a, b) => (a.toInt, (r.toInt, b.toInt))
                            }
                         })
                        .partitionBy(hashPartitioner)
                        .setName("type4Axioms")
                        .persist(StorageLevel.MEMORY_AND_DISK)    
    type4Axioms.count()     
    
    val type4AxiomsCompoundKey = type4Axioms.map({ case (a, (r, b)) => ((r, a), b) })
                                            .partitionBy(hashPartitioner)
                                            .setName("type4AxiomsCompoundKey")
                                            .persist(StorageLevel.MEMORY_AND_DISK)
    type4AxiomsCompoundKey.count()                                        
      
    val type5Axioms = sc.textFile(dirPath + "Type5Axioms.txt")
                        .map[(Int, Int)](line => {
                            line.split("\\|") match {
                            case Array(r, s) => (r.toInt, s.toInt)
                            }
                         })
                        .partitionBy(hashPartitioner)
                        .setName("type5Axioms")
                        .persist(StorageLevel.MEMORY_AND_DISK)
    type5Axioms.count()
      
    val type6Axioms = sc.textFile(dirPath + "Type6Axioms.txt")
                        .map[(Int, (Int, Int))](line => {
                            line.split("\\|") match {
                            case Array(r, s, t) => (r.toInt, (s.toInt, t.toInt))
                            }
                         })
                        .partitionBy(hashPartitioner)
                        .setName("type6Axioms")
                        .persist(StorageLevel.MEMORY_AND_DISK)
                        
     type6Axioms.count()                   

    //return the initialized RDDs as a Tuple object (can have at max 22 elements in Spark Tuple)
    (uAxioms, uAxiomsFlipped, rAxioms, type1Axioms, type2Axioms, type2AxiomsConjunctsFlipped, 
        type3Axioms, type4Axioms, type4AxiomsCompoundKey, type5Axioms, type6Axioms)
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
    var r2Join2 = r2Join21.union(r2Join22)
                          .setName("r2Join2_" + loopCounter)

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
    val r4Join1 = type4Axioms.join(filteredUAxioms) 
           
    val r4Join1YKey = r4Join1.map({ case (a, ((r1, b), y)) => ((r1, y), b) })
                             .partitionBy(hashPartitioner)
    val rAxiomsPairYKey = rAxioms.map({ case (r2, (x, y)) => ((r2, y), x) })
                                 .partitionBy(hashPartitioner)
    val r4Join2 = r4Join1YKey.join(rAxiomsPairYKey)

    val r4Result = r4Join2.map({ case ((r, y), (b, x)) => (b, x) })
                          .partitionBy(hashPartitioner)
   
    r4Result
   }
  
  def completionRule4_delta(sc: SparkContext, 
      filteredDeltaUAxioms: RDD[(Int, Int)], filteredUAxioms: RDD[(Int, Int)], 
      filteredUAxiomsFlipped: RDD[(Int, Int)], 
      filteredDeltaRAxioms: RDD[(Int, (Int, Int))], 
      filteredRAxioms: RDD[(Int, (Int, Int))], 
      type4Axioms: RDD[(Int, (Int, Int))], 
      type4AxiomsCompoundKey: RDD[((Int, Int), Int)]): RDD[(Int, Int)] = { 
    
//    if(filteredDeltaUAxioms.isEmpty() && filteredDeltaRAxioms.isEmpty())
//      return sc.emptyRDD[(Int, Int)]
    
    // part-1 (using deltaU and fullR)
    val r4Join1P1 = type4Axioms.join(filteredDeltaUAxioms)            
    val r4Join1CompoundKey = r4Join1P1.map({ case (a, ((r1, b), y)) => ((r1, y), b) })
                                      .partitionBy(hashPartitioner)
    val rAxiomsPairYKey = filteredRAxioms.map({ case (r2, (x, y)) => ((r2, y), x) })
                                         .partitionBy(hashPartitioner)
    val r4Join2P1 = r4Join1CompoundKey.join(rAxiomsPairYKey)
    val r4Result1 = r4Join2P1.map({ case ((r, y), (b, x)) => (b, x) })
                             .partitionBy(hashPartitioner)
    
    // part-2 (using deltaR and fullU)                      
//    if(filteredDeltaRAxioms.isEmpty())
//      return r4Result1      
    val deltaRAxiomsYKey = filteredDeltaRAxioms.map({ case (r, (x, y)) => (y, (r, x)) })
                                               .partitionBy(hashPartitioner)           
    val r4Join1P2 = deltaRAxiomsYKey.join(filteredUAxiomsFlipped)
    val r4Join1P2CompoundKey = r4Join1P2.map({ case (y, ((r, x), a)) => ((r, a), x) })
                                        .partitionBy(hashPartitioner)
    val r4Join2P2 = r4Join1P2CompoundKey.join(type4AxiomsCompoundKey)
    val r4Result2 = r4Join2P2.map({ case ((r, a), (x, b)) => (b, x) })
                             .partitionBy(hashPartitioner)
                             
    val r4Result = r4Result1.union(r4Result2)                         
                                        
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
  def completionRule6_delta(sc: SparkContext, type6R1: Set[Int], type6R2: Set[Int], deltaRAxioms: RDD[(Int, (Int, Int))], rAxioms: RDD[(Int, (Int, Int))], type6Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {

    //filter rAxioms on r1 and r2 found in type6Axioms
    val delRAxiomsFilterOnR1 = deltaRAxioms.filter{case (r1, (x, y)) => type6R1.contains(r1)}
    
     
    val r6Join11 = type6Axioms.join(delRAxiomsFilterOnR1)
                             .map({ case (r1, ((r2, r3), (x, y))) => ((r2, y), (r3, x)) })
                             .partitionBy(hashPartitioner)  
                             
//    if(r6Join11.isEmpty())
//        return sc.emptyRDD
    
    val rAxiomsFilterOnR2 = rAxioms.filter{case (r2, (y, z)) => type6R2.contains(r2)}
                                     .map({ case (r2, (y, z)) => ((r2, y), z)}) //for r6Join2
                                     .partitionBy(hashPartitioner)
    
 //   val rAxiomsFilterOnR2Count = rAxiomsFilterOnR2.count()
    
//    println("rAxiomsFilterOnR2Count: "+rAxiomsFilterOnR2Count)
//    if(rAxiomsFilterOnR2Count == 0)
//      return sc.emptyRDD
   
    //Join1 - joins on r  
      
    //Join2 - joins on compound key
    val r6Join12 = r6Join11.join(rAxiomsFilterOnR2) // ((r2,y),((r3,x),z))
                         .values // ((r3,x),z)
                         .map( { case ((r3,x),z) => (r3,(x,z))})
                         .partitionBy(hashPartitioner)
 
  // println("r6Join12: "+r6Join12.count())  
  
   //--------------------Reverse filtering of deltaR and rAxioms------------------------------------------------
   
   val delRAxiomsFilterOnR2 = deltaRAxioms.filter{case (r2, (x, y)) => type6R2.contains(r2)}
    
 //Join1 - joins on r 
   val type6AxiomsFlippedConjuncts = type6Axioms.map({case (r1,(r2,r3)) => (r2,(r1,r3))})
                                                 .partitionBy(hashPartitioner) 
    
   val r6Join21 = type6AxiomsFlippedConjuncts.join(delRAxiomsFilterOnR2)
                             .map({ case (r2,((r1,r3), (x, y))) => ((r1, y), (r3, x)) })
                             .partitionBy(hashPartitioner)

//   if(r6Join21.isEmpty())
//       return sc.emptyRDD
                             
    
   val rAxiomsFilterOnR1 = rAxioms.filter{case (r1, (y, z)) => type6R2.contains(r1)}
                                     .map({ case (r1, (y, z)) => ((r1, y), z)}) //for r6Join2
                                     .partitionBy(hashPartitioner)
    
//    val rAxiomsFilterOnR1Count = rAxiomsFilterOnR1.count()
//    
//    println("rAxiomsFilterOnR1Count: "+rAxiomsFilterOnR1Count)
//    if(rAxiomsFilterOnR1Count == 0)
//      return sc.emptyRDD
   
   
      
    //Join2 - joins on compound key
    val r6Join22 = r6Join21.join(rAxiomsFilterOnR1) // ((r1, y), ((r3, x),z))
                         .values // ((r3,x),z)
                         .map( { case ((r3,x),z) => (r3,(x,z))})
                         .partitionBy(hashPartitioner)
 
   //println("r6Join22: "+r6Join22.count()) 
   
   val r6JoinFinal = r6Join12.union(r6Join22) 
                         
   r6JoinFinal
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
      sc.union(prevDeltaURule1, prevDeltaURule2, prevDeltaURule4)
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
        sc.union(currDeltaURule1, prevDeltaURule2, prevDeltaURule4)
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
         sc.union(currDeltaURule1, currDeltaURule2, prevDeltaURule4)
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
      rAxiomsRule3: RDD[(Int, (Int, Int))], prevDeltaRRule6: RDD[(Int, (Int, Int))], prevDeltaRRule5: RDD[(Int, (Int, Int))], 
      currDeltaRRule3: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {
    val deltaRAxiomsToRule5 = { 
      if (loopCounter == 1)
        rAxiomsRule3
      else
        sc.union(prevDeltaRRule5, prevDeltaRRule6, currDeltaRRule3)
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

    var (uAxioms, uAxiomsFlipped, rAxioms, type1Axioms, type2Axioms, 
        type2AxiomsConjunctsFlipped, type3Axioms, type4Axioms, type4AxiomsCompoundKey, 
        type5Axioms, type6Axioms) = initializeRDD(sc, dirPath)
      
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
    val type4Fillers = type4Axioms.collect().map({ case (a, (r, b)) => a }).toSet
    val type4FillersBroadcast = { 
      if (!type4Fillers.isEmpty)
        sc.broadcast(type4Fillers) 
      else 
        null
      }
    // used for filtering of raxioms in rule4
    val type4Roles = type4Axioms.collect().map({ case (a, (r, b)) => r }).toSet
    val type4RolesBroadcast = { 
      if (!type4Roles.isEmpty)
        sc.broadcast(type4Roles) 
      else 
        null
      }
    //bcast r1 and r2 for filtering in rule6
    val type6R1 = type6Axioms.collect()
                             .map({ case (r1, (r2, r3)) => r1})
                             .toSet
    
    val type6R1Bcast = sc.broadcast(type6R1)
    
    val type6R2 = type6Axioms.collect()
                             .map({ case (r1, (r2, r3)) => r2})
                             .toSet
   
    val type6R2Bcast = sc.broadcast(type6R2)
   //end of bcast for rule6  

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
              case (k, v) => type4FillersBroadcast.value.contains(k) }).partitionBy(hashPartitioner) 
        else
          sc.emptyRDD[(Int, Int)]
        }  
      currDeltaURule4 = completionRule4(filteredUAxiomsRule2, rAxiomsRule3, type4Axioms) 
          
      var uAxiomsRule4 = uAxiomsRule2.union(currDeltaURule4)
      uAxiomsRule4 = customizedDistinctForUAxioms(uAxiomsRule4)
                                     .setName("uAxiomsRule4_" + loopCounter)  
                                     
      //get delta U for only the current iteration                               
      currDeltaURule4 = uAxiomsRule4.subtract(uAxiomsRule2, hashPartitioner)                                         

/*      
      val filteredCurrDeltaURule2 = { 
        if (type4FillersBroadcast != null)
          currDeltaURule2.filter({ 
              case (a, x) => type4FillersBroadcast.value.contains(a) })
        else
          sc.emptyRDD[(Int, Int)]
      }  
      val filteredUAxiomsRule2 = { 
        if (type4FillersBroadcast != null)
          uAxiomsRule2.filter({ 
              case (a, x) => type4FillersBroadcast.value.contains(a) })
        else
          sc.emptyRDD[(Int, Int)]
      }
      // filtering on uAxiomsFlipped instead of flipping and partitioning on
      // filteredUAxiomsRule2 to avoid a shuffle operation (partitioning)
      val filteredUAxiomsFlippedRule2 = { 
        if (type4FillersBroadcast != null)
          uAxiomsFlipped.filter({ 
              case (x, a) => type4FillersBroadcast.value.contains(a) })
        else
          sc.emptyRDD[(Int, Int)]
      }
/*      
      val filteredCurrDeltaRRule3 = {
        if (type4RolesBroadcast != null)
          currDeltaRRule3.filter({ 
              case (r, (a, b)) => type4RolesBroadcast.value.contains(r) })
        else
          sc.emptyRDD[(Int, (Int, Int))]    
      }
      
      val filteredRAxiomsRule3 = {
        if (type4RolesBroadcast != null)
          rAxiomsRule3.filter({ 
              case (r, (a, b)) => type4RolesBroadcast.value.contains(r) })
        else
          sc.emptyRDD[(Int, (Int, Int))]    
      }
      currDeltaURule4 = completionRule4_delta(sc, filteredCurrDeltaURule2, 
          filteredUAxiomsRule2, filteredUAxiomsFlippedRule2, filteredCurrDeltaRRule3, 
          filteredRAxiomsRule3, type4Axioms, type4AxiomsCompoundKey)
*/
      currDeltaURule4 = completionRule4_delta(sc, filteredCurrDeltaURule2, 
          filteredUAxiomsRule2, filteredUAxiomsFlippedRule2, currDeltaRRule3, 
          rAxiomsRule3, type4Axioms, type4AxiomsCompoundKey)

      var uAxiomsRule4 = uAxiomsRule2.union(currDeltaURule4)
      uAxiomsRule4 = customizedDistinctForUAxioms(uAxiomsRule4)
                                     .setName("uAxiomsRule4_" + loopCounter)      
*/
//      println("----Completed rule4----")                               
      
      //Rule 5 
      val deltaRAxiomsToRule5 = prepareRule5Inputs(loopCounter, sc, rAxiomsRule3, prevDeltaRRule6, 
                                                 prevDeltaRRule5, currDeltaRRule3)
      var currDeltaRRule5 = completionRule5(deltaRAxiomsToRule5, type5Axioms) //Rule5  
      println("----Completed rule5----")
                   
      var rAxiomsRule5 = rAxiomsRule3.union(currDeltaRRule5)
      rAxiomsRule5 = customizedDistinctForRAxioms(rAxiomsRule5).setName("rAxiomsRule5_" + loopCounter) 
       
      val deltaRAxiomsToRule6 = {
         if(loopCounter == 1)
           rAxiomsRule5           
         else 
           sc.union(prevDeltaRRule6, currDeltaRRule3, currDeltaRRule5)
          // sc.union(currDeltaRRule3, currDeltaRRule5)  
             .partitionBy(hashPartitioner)
       }
       
       var currDeltaRRule6 = completionRule6_delta(sc, type6R1Bcast.value, type6R2Bcast.value, deltaRAxiomsToRule6 ,rAxiomsRule5, type6Axioms)
       println("----Completed rule6----")
       
       var rAxiomsRule6: RDD[(Int, (Int, Int))] = sc.emptyRDD
       
//       if(currDeltaRRule6.isEmpty()){
//         rAxiomsRule6 = rAxiomsRule5
//       }
//       else{
//        rAxiomsRule6 = rAxiomsRule5.union(currDeltaRRule6)
//                                   .partitionBy(hashPartitioner)
//        rAxiomsRule6 = customizedDistinctForRAxioms(rAxiomsRule6).setName("rAxiomsRule6_"+loopCounter)
//       }
       
      rAxiomsRule6 = rAxiomsRule5.union(currDeltaRRule6)
                                   .partitionBy(hashPartitioner)
      rAxiomsRule6 = customizedDistinctForRAxioms(rAxiomsRule6).setName("rAxiomsRule6_"+loopCounter)
      
      //TODO: update final variables
      uAxiomsFinal = uAxiomsRule4
      rAxiomsFinal = rAxiomsRule6
      
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
      println("rAxiomCount: " + currRAxiomsCount + ", Time taken for rAxiom count: " + 
          (t_end_rAxiomCount - t_begin_rAxiomCount) / 1e9 + " s")
      println("====================================")
      
      
      
          
      //delta RDDs
      currDeltaURule1 = currDeltaURule1.setName("currDeltaURule1_" + loopCounter)
                                       .persist(StorageLevel.MEMORY_AND_DISK)
      println("currDeltaURule1_" + loopCounter+": "+currDeltaURule1.count())                               
                                       
      currDeltaURule2 = currDeltaURule2.setName("currDeltaURule2_" + loopCounter)
                                       .persist(StorageLevel.MEMORY_AND_DISK)
      
      println("currDeltaURule2_" + loopCounter+": "+currDeltaURule2.count())
      
      uAxiomsFlipped = uAxiomsFlipped.setName("uAxiomsFlipped_" + loopCounter)
                                     .persist(StorageLevel.MEMORY_AND_DISK)
                                     
      println("uAxiomsFlipped_" + loopCounter+": "+uAxiomsFlipped.count()) 
      
      currDeltaRRule3 = currDeltaRRule3.setName("currDeltaRRule3_"+loopCounter)
                                       .persist(StorageLevel.MEMORY_AND_DISK)
      
      println("currDeltaRRule3_" + loopCounter+": "+currDeltaRRule3.count())
      
     
      currDeltaURule4 = currDeltaURule4.setName("currDeltaURule4_" + loopCounter)
                                       .persist(StorageLevel.MEMORY_AND_DISK)
      
      println("currDeltaURule4_" + loopCounter+": "+currDeltaURule4.count())
      
      
      currDeltaRRule5 = currDeltaRRule5.setName("currDeltaRRule5_" + loopCounter)
                                       .persist(StorageLevel.MEMORY_AND_DISK)
                                       
      println("currDeltaRRule5_" + loopCounter+": "+currDeltaRRule5.count())
      
      currDeltaRRule6 = currDeltaRRule6.setName("currDeltaRRule6_" + loopCounter)
                                       .persist(StorageLevel.MEMORY_AND_DISK)
                                       
      println("currDeltaRRule6_" + loopCounter+": "+currDeltaRRule6.count())
      
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
      prevDeltaRRule6 = currDeltaRRule6
      
//     if(loopCounter == 6) 
//        Thread.sleep(3000000)

    }
   
     val t_end = System.nanoTime()
     println("Total time taken for the program: "+ (t_end - t_init)/ 1e9 + " s")
     
     
     Thread.sleep(3000000) // add 100s delay for UI vizualization

    sc.stop()

  }

}