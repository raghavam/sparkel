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

object SparkELDAGAnalysis {

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
     
   //  val type6AxiomsConjunctsFlipped = type6Axioms.map({case ((r1, r2), r3) => ((r2, r1), r3)})

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
  def completionRule1(uAxioms: RDD[(Int, Int)], type1Axioms: RDD[(Int, Int)], loopCounter: Int): RDD[(Int, Int)] = {

    val r1Join = type1Axioms.join(uAxioms)
                            .values
                            .distinct(numPartitions)
                            .partitionBy(hashPartitioner)
                           
    // uAxioms is immutable as it is input parameter, so use new constant uAxiomsNew
//    val uAxiomsNew = uAxioms.union(r1Join)   //union is partitioner aware
//                            .setName("uAxiomsRule1_"+loopCounter)
//                           .persist()
    r1Join
  }
  
   //completion rule1
  def completionRule1_delta(deltaUAxioms: RDD[(Int, Int)], type1Axioms: RDD[(Int, Int)], 
      loopCounter: Int): RDD[(Int, Int)] = {

    val r1Join = type1Axioms.join(deltaUAxioms)
                            .values
//                            .distinct(numPartitions)
                            .partitionBy(hashPartitioner)
                              
    r1Join
  }
  

  def completionRule2_deltaNew(loopCounter: Int, type2A1A2: Broadcast[Set[(Int, Int)]], 
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
//                                      .persist()
    
    //r2JoinFilterMap.count() //to force persist                                  
    
    // val type2AxiomsMap1 = type2Axioms.map({case(a1,(a2,b)) => ((a1,a2),b)}).partitionBy(type2Axioms.partitioner.get).persist()
    val r2Join21 = r2JoinFilterMap.join(type2Axioms)
                                   .map({ case ((a1, a2), (x, b)) => (b, x) })
 //                                 .repartition(numPartitions)
                                  .partitionBy(hashPartitioner)
                                  .setName("r2Join21_" + loopCounter)
//                                  .persist()
    //JOIN 2 - PART 2
    
    // val type2AxiomsMap2 = type2Axioms.map({case(a1,(a2,b)) => ((a2,a1),b)}).partitionBy(type2Axioms.partitioner.get).persist()
    val r2Join22 = r2JoinFilterMap.join(type2AxiomsConjunctsFlipped)
                                  .map({ case ((a1, a2), (x, b)) => (b, x) })
//                                  .repartition(numPartitions)
                                  .partitionBy(hashPartitioner)
                                  .setName("r2Join22_" + loopCounter)
//                                  .persist()
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
  
  //completion rule 5
  def completionRule5(deltaRAxioms: RDD[(Int, (Int, Int))], type5Axioms: RDD[(Int, Int)]): RDD[(Int, (Int, Int))] = {
    val r5Join = type5Axioms.join(deltaRAxioms)
                            .map({ case (k, (v1, (v2, v3))) => (v1, (v2, v3)) })
                            .partitionBy(hashPartitioner)

    r5Join
  }
  
  
  //completion rule 6
  def completionRule6_compoundKeys(sc: SparkContext, type6R1: Set[Int], type6R2: Set[Int], rAxioms: RDD[(Int, (Int, Int))], type6Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {

    //filter rAxioms on r1 and r2 found in type6Axioms
    val rAxiomsFilteredOnR1 = rAxioms.filter{case (r1, (x, y)) => type6R1.contains(r1)}
    
    //debug
    println("rAxiomsFilteredOnR1: "+rAxiomsFilteredOnR1.count())
    
    val rAxiomsFilteredOnR2 = rAxioms.filter{case (r2, (y, z)) => type6R2.contains(r2)}
                                     .map({ case (r2, (y, z)) => ((r2, y), z)}) //for r6Join2
                                     .partitionBy(hashPartitioner)
    
    println("rAxiomsFilteredOnR2: "+rAxiomsFilteredOnR2.count())
   
    //Join1 - joins on r                                
    val r6Join1 = type6Axioms.join(rAxiomsFilteredOnR1)
                             .map({ case (r1, ((r2, r3), (x, y))) => ((r2, y), (r3, x)) })
                             .partitionBy(hashPartitioner)

    println("r6Join1: "+r6Join1.count())
      
    //Join2 - joins on compound key
    val r6Join2 = r6Join1.join(rAxiomsFilteredOnR2) // ((r2,y),((r3,x),z))
                         .values // ((r3,x),z)
                         .map( { case ((r3,x),z) => (r3,(x,z))})
                         .partitionBy(hashPartitioner)
 
   println("r6Join2: "+r6Join2.count())                      
      
   r6Join2
  }
  
  //completion rule 6
  def completionRule6_delta(sc: SparkContext, type6R1: Set[Int], type6R2: Set[Int], deltaRAxioms: RDD[(Int, (Int, Int))], rAxioms: RDD[(Int, (Int, Int))], type6Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, (Int, Int))] = {

    //filter rAxioms on r1 and r2 found in type6Axioms
    val delRAxiomsFilterOnR1 = deltaRAxioms.filter{case (r1, (x, y)) => type6R1.contains(r1)}
    
    val delRAxiomsFilterOnR1Count = delRAxiomsFilterOnR1.count()
        
    //debug
    println("delRAxiomsFilterOnR1Count: "+delRAxiomsFilterOnR1Count)
    if(delRAxiomsFilterOnR1Count == 0)
      return sc.emptyRDD       
    
    val rAxiomsFilterOnR2 = rAxioms.filter{case (r2, (y, z)) => type6R2.contains(r2)}
                                     .map({ case (r2, (y, z)) => ((r2, y), z)}) //for r6Join2
                                     .partitionBy(hashPartitioner)
    
    val rAxiomsFilterOnR2Count = rAxiomsFilterOnR2.count()
    
    println("rAxiomsFilterOnR2Count: "+rAxiomsFilterOnR2Count)
    if(rAxiomsFilterOnR2Count == 0)
      return sc.emptyRDD
   
    //Join1 - joins on r  
      
    
    val r6Join11 = type6Axioms.join(delRAxiomsFilterOnR1)
                             .map({ case (r1, ((r2, r3), (x, y))) => ((r2, y), (r3, x)) })
                             .partitionBy(hashPartitioner)

    val r6Join11Count = r6Join11.count()                         
    println("r6Join1Count: "+r6Join11Count)
    if(r6Join11Count == 0)
      return sc.emptyRDD
      
    //Join2 - joins on compound key
    val r6Join12 = r6Join11.join(rAxiomsFilterOnR2) // ((r2,y),((r3,x),z))
                         .values // ((r3,x),z)
                         .map( { case ((r3,x),z) => (r3,(x,z))})
                         .partitionBy(hashPartitioner)
 
   println("r6Join12: "+r6Join12.count())  
  
   //--------------------Reverse filtering of deltaR and rAxioms------------------------------------------------
   
   val delRAxiomsFilterOnR2 = deltaRAxioms.filter{case (r2, (x, y)) => type6R2.contains(r2)}
    
   val delRAxiomsFilterOnR2Count = delRAxiomsFilterOnR2.count()
        
    //debug
    println("delRAxiomsFilterOnR2Count: "+delRAxiomsFilterOnR2Count)
    if(delRAxiomsFilterOnR2Count == 0)
      return sc.emptyRDD       
    
    val rAxiomsFilterOnR1 = rAxioms.filter{case (r1, (y, z)) => type6R2.contains(r1)}
                                     .map({ case (r1, (y, z)) => ((r1, y), z)}) //for r6Join2
                                     .partitionBy(hashPartitioner)
    
    val rAxiomsFilterOnR1Count = rAxiomsFilterOnR1.count()
    
    println("rAxiomsFilterOnR1Count: "+rAxiomsFilterOnR1Count)
    if(rAxiomsFilterOnR1Count == 0)
      return sc.emptyRDD
   
    //Join1 - joins on r 
    val type6AxiomsFlippedConjuncts = type6Axioms.map({case (r1,(r2,r3)) => (r2,(r1,r3))}) 
    
    val r6Join21 = type6Axioms.join(delRAxiomsFilterOnR2)
                             .map({ case (r2,((r1,r3), (x, y))) => ((r1, y), (r3, x)) })
                             .partitionBy(hashPartitioner)

    val r6Join21Count = r6Join21.count()                         
    println("r6Join21Count: "+r6Join21Count)
    if(r6Join21Count == 0)
      return sc.emptyRDD
      
    //Join2 - joins on compound key
    val r6Join22 = r6Join21.join(rAxiomsFilterOnR1) // ((r1, y), ((r3, x),z))
                         .values // ((r3,x),z)
                         .map( { case ((r3,x),z) => (r3,(x,z))})
                         .partitionBy(hashPartitioner)
 
   //println("r6Join22: "+r6Join22.count()) 
   
      
   r6Join22
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

  //Deletes the exisiting output directory
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
    var prevRAxiomsRule5: RDD[(Int, (Int, Int))] = sc.emptyRDD
    var prevUAxiomsFlipped = uAxiomsFlipped
    var prevUAxiomsFinal= uAxioms
    var prevRAxiomsFinal = rAxioms
    

    //for pre-filtering for rule2
    val type2Collect = type2Axioms.collect()
    val type2FillersA1A2 = type2Collect.map({ case ((a1, a2), b) => (a1, a2) }).toSet
    val type2ConjunctsBroadcast = sc.broadcast(type2FillersA1A2)
    
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
      var t_begin_rule = System.nanoTime()
       val deltaUAxiomsForRule1 = {
        if (loopCounter == 1)
          uAxiomsFinal
        else
          //sc.union(prevDeltaURule2, prevDeltaURule4, currDeltaURule1)
          sc.union(prevDeltaURule1, prevDeltaURule2)
            .partitionBy(hashPartitioner) //if rule4 is not yet implemented, do not include prevDeltaURule4 in union
            .setName("deltaUAxiomsForRule2_" + loopCounter)
      } 
      var currDeltaURule1 = completionRule1_delta(deltaUAxiomsForRule1, type1Axioms, loopCounter)
     // currDeltaURule1 = currDeltaURule1.setName("deltaURule1_"+loopCounter).persist(StorageLevel.MEMORY_AND_DISK)
     // currDeltaURule1.count() // to force persist()
      var t_end_rule = System.nanoTime()
      println("----Completed rule1----")
      // println("count: "+ uAxiomRule1Count+" Time taken: "+ (t_end_rule - t_begin_rule) / 1e6 + " ms")
      println("=====================================")

      
      
      //Prepare input to Rule2      
     // currDeltaURule1 = uAxiomsRule1.subtract(uAxiomsFinal)
     //                               .setName("currDeltaURule1_"+loopCounter)
      
      var uAxiomsRule1 = uAxiomsFinal.union(currDeltaURule1)
      uAxiomsRule1 = customizedDistinctForUAxioms(uAxiomsRule1).setName("uAxiomsRule1_" + loopCounter)
  //                                   .persist()
     
      val deltaUAxiomsForRule2 = {
        if (loopCounter == 1)
          uAxiomsRule1 //uAxiom total for first loop
        else
          //sc.union(prevDeltaURule2, prevDeltaURule4, currDeltaURule1)
          sc.union(prevDeltaURule2, currDeltaURule1)
            .partitionBy(hashPartitioner) //if rule4 is not yet implemented, do not include prevDeltaURule4 in union
            .setName("deltaUAxiomsForRule2_" + loopCounter)
      }
      
      
      //flip delta uAxioms
      val deltaUAxiomsFlipped = deltaUAxiomsForRule2.map({ case (a, x) => (x, a) }) 
                                                    .partitionBy(hashPartitioner)
                                                    .setName("deltaUAxiomsFlipped_"+loopCounter)
//                                                    .persist()
      //update uAxiomsFlipped
      uAxiomsFlipped = sc.union(uAxiomsFlipped, deltaUAxiomsFlipped).partitionBy(hashPartitioner)
      uAxiomsFlipped = customizedDistinctForUAxioms(uAxiomsFlipped).setName("uAxiomsFlipped_"+loopCounter)
                  
      //End of Prepare input to Rule2 
                                                                              
                                                                              
      //execute Rule 2
      t_begin_rule = System.nanoTime()
      var currDeltaURule2 = completionRule2_deltaNew(loopCounter, type2ConjunctsBroadcast, deltaUAxiomsForRule2, uAxiomsFlipped, type2Axioms, type2AxiomsConjunctsFlipped)
//      currDeltaURule2 = currDeltaURule2.setName("deltaURule2_"+loopCounter).persist(StorageLevel.MEMORY_AND_DISK)
      t_end_rule = System.nanoTime()
      println("----Completed rule2----")
      //println("count: "+ uAxiomRule2Count+" Time taken: "+ (t_end_rule - t_begin_rule) / 1e6 + " ms")
      println("=====================================")

       var uAxiomsRule2 = uAxiomsRule1.union(currDeltaURule2)
       uAxiomsRule2 = customizedDistinctForUAxioms(uAxiomsRule2).setName("uAxiomsRule2_" + loopCounter)                             
       
       
      //Prepare input for rule3 
       
        val deltaUAxiomsForRule3 = { 
         if (loopCounter == 1)
           uAxiomsRule2
         else
           //sc.union(prevDeltaURule4, currDeltaURule1, currDeltaURule2)
           sc.union(currDeltaURule1, currDeltaURule2) //if rule4 is not yet implemented, do not include prevDeltaURule4 in union
             .partitionBy(hashPartitioner) 
             .setName("deltaUAxiomsForRule3_" + loopCounter)
         }

       t_begin_rule = System.nanoTime()
       var currDeltaRRule3 = completionRule3(deltaUAxiomsForRule3, type3Axioms) //Rule3
       t_end_rule = System.nanoTime()
       println("----Completed rule3----")
       
       var rAxiomsRule3 = {
         if(loopCounter ==1)
           currDeltaRRule3
         else
           rAxiomsFinal.union(currDeltaRRule3) //partitionAware union
       }
       
       rAxiomsRule3 = customizedDistinctForRAxioms(rAxiomsRule3).setName("rAxiomsRule3_" + loopCounter)
       
       //prepare input to Rule 5
       
       val deltaRAxiomsToRule5 = { 
         if (loopCounter == 1)
           rAxiomsRule3
         else
          // sc.union(prevDeltaRRule5, prevDeltaRRule6, currDeltaRRule3)
           sc.union(prevDeltaRRule6, prevDeltaRRule5, currDeltaRRule3)
             .partitionBy(hashPartitioner)
         }

       t_begin_rule = System.nanoTime()
       var currDeltaRRule5 = completionRule5(deltaRAxiomsToRule5, type5Axioms) //Rule5  
       t_end_rule = System.nanoTime()
       println("----Completed rule5----")
       
             
       var rAxiomsRule5 = rAxiomsRule3.union(currDeltaRRule5)
       rAxiomsRule5 = customizedDistinctForRAxioms(rAxiomsRule5).setName("rAxiomsRule5_" + loopCounter) 
       
       val deltaRAxiomsToRule6 = {
         if(loopCounter == 1)
           rAxiomsRule5
           
         else 
           sc.union(prevDeltaRRule6, currDeltaRRule3, currDeltaRRule5)
             .partitionBy(hashPartitioner)
       }
       
       t_begin_rule = System.nanoTime()
       var currDeltaRRule6 = completionRule6_delta(sc, type6R1Bcast.value, type6R2Bcast.value, deltaRAxiomsToRule6 ,rAxiomsRule5, type6Axioms) 
       t_end_rule = System.nanoTime() 
       println("----Completed rule6----")
       
       var rAxiomsRule6 = rAxiomsRule5.union(currDeltaRRule6)
                                      .partitionBy(hashPartitioner)
       rAxiomsRule6 = customizedDistinctForRAxioms(rAxiomsRule6).setName("rAxiomsRule6_"+loopCounter)
       
       
      //TODO: update final vars to the last rule's vars for next iteration 
      uAxiomsFinal = uAxiomsRule2
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
      
      
      
      //prev RDD assignments
      prevUAxiomsFinal.unpersist()
      prevUAxiomsFinal = uAxiomsFinal
      prevRAxiomsFinal.unpersist()
      prevRAxiomsFinal = rAxiomsFinal
      
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
      
      currDeltaRRule5 = currDeltaRRule5.setName("currDeltaRRule5_"+loopCounter)
                                       .persist(StorageLevel.MEMORY_AND_DISK)
                                       
      currDeltaRRule5.count()
      
      currDeltaRRule6 = currDeltaRRule6.setName("currDeltaRRule6_"+loopCounter)
                                       .persist(StorageLevel.MEMORY_AND_DISK)
                                       
      currDeltaRRule6.count()
      
      rAxiomsRule5 = rAxiomsRule5.setName("rAxiomsRule5_"+loopCounter)
                                 .persist(StorageLevel.MEMORY_AND_DISK)
      
      rAxiomsRule5.count()
      
      //prev delta RDDs assignments
      prevDeltaURule1.unpersist()
      prevDeltaURule1 = currDeltaURule1
      prevDeltaURule2.unpersist()                                      
      prevDeltaURule2 = currDeltaURule2
      prevUAxiomsFlipped.unpersist()
      prevUAxiomsFlipped = uAxiomsFlipped
      prevDeltaRRule3.unpersist()
      prevDeltaRRule3 = currDeltaRRule3
      prevDeltaRRule5.unpersist()
      prevDeltaRRule5 = currDeltaRRule5
      prevDeltaRRule6.unpersist()
      prevDeltaRRule6 = currDeltaRRule6
      prevRAxiomsRule5.unpersist()
      prevRAxiomsRule5 = rAxiomsRule5
      
      

    }
   
     val t_end = System.nanoTime()
     println("Total time taken for the program: "+ (t_end - t_init)/ 1e9 + " s")
     
     
     Thread.sleep(3000000) // add 100s delay for UI vizualization

    sc.stop()

  }

}