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

  var numPartitions = -1 // later insitialized from commandline
  var hashPartitioner: HashPartitioner = null

  /*
   * Initializes all the RDDs corresponding to each axiom-type. 
   * Note, we want all the typeAxioms to reside in cache() and not on disk, 
   * hence we are using the default persistence level, i.e. Memory_Only.
   */
  def initializeRDD(sc: SparkContext, dirPath: String) = {

    
     val sAxioms = sc.textFile(dirPath + "sAxioms.txt").map[(Int, Int)](line => { line.split("\\|") match { case Array(x, y) => (x.toInt, y.toInt) }})
                                                      .partitionBy(hashPartitioner)
                                                      .setName("sAxioms")
                                                      
      
     sAxioms.persist().count()
     sAxioms.unpersist().count()
    
     val uAxioms = sc.textFile(dirPath + "sAxioms.txt")
                     .map[(Int, Int)](line => { line.split("\\|") match { case Array(x, y) => (y.toInt, x.toInt) }})
                     .partitionBy(hashPartitioner)
                     .setName("uAxioms")
                     .persist(StorageLevel.MEMORY_AND_DISK)
      
     uAxioms.count()

    val rAxioms: RDD[(Int, (Int, Int))] = sc.emptyRDD

    val type1Axioms = sc.textFile(dirPath + "Type1Axioms.txt")
                        .map[(Int, Int)](line => {line.split("\\|") match { case Array(x, y) => (x.toInt, y.toInt)}})
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
                        .map[(Int, (Int, Int))](line => {line.split("\\|") match {
                            case Array(x, y, z) => (x.toInt, (y.toInt, z.toInt))
                              }
                         })
                        .partitionBy(hashPartitioner)
                        .setName("type2Axioms")
                        .persist(StorageLevel.MEMORY_AND_DISK)
                        
    type2Axioms.count()     

    val type2AxiomsMap1 = type2Axioms.map({ case (a1, (a2, b)) => ((a1, a2), b) })
                                     .partitionBy(hashPartitioner)
                                     .setName("type2AxiomsMap1")
                                     .persist(StorageLevel.MEMORY_AND_DISK)
                                     
    type2AxiomsMap1.count()
                          
    val type2AxiomsMap2 = type2Axioms.map({ case (a1, (a2, b)) => ((a2, a1), b) })
                                     .partitionBy(hashPartitioner)
                                     .setName("type2AxiomsMap2")
                                     .persist(StorageLevel.MEMORY_AND_DISK)
                                     
    type2AxiomsMap2.count()

    val type3Axioms = sc.textFile(dirPath + "Type3Axioms.txt")
      .map[(Int, (Int, Int))](line => {
        line.split("\\|") match {
          case Array(x, y, z) => (x.toInt, (y.toInt, z.toInt))
        }
      })
      .partitionBy(hashPartitioner)
      .setName("type3Axioms").persist(StorageLevel.MEMORY_AND_DISK)
      
    val type4Axioms = sc.textFile(dirPath + "Type4Axioms.txt")
      .map[(Int, (Int, Int))](line => {
        line.split("\\|") match {
          case Array(x, y, z) => (x.toInt, (y.toInt, z.toInt))
        }
      })
      .partitionBy(hashPartitioner)
      .setName("type4Axioms").persist(StorageLevel.MEMORY_AND_DISK)
      
    val type5Axioms = sc.textFile(dirPath + "Type5Axioms.txt")
      .map[(Int, Int)](line => {
        line.split("\\|") match {
          case Array(x, y) => (x.toInt, y.toInt)
        }
      })
      .partitionBy(hashPartitioner)
      .setName("type5Axioms").persist(StorageLevel.MEMORY_AND_DISK)
      
    val type6Axioms = sc.textFile(dirPath + "Type6Axioms.txt")
      .map[(Int, (Int, Int))](line => {
        line.split("\\|") match {
          case Array(x, y, z) => (x.toInt, (y.toInt, z.toInt))
        }
      })
      .partitionBy(hashPartitioner)
      .setName("type6Axioms").persist(StorageLevel.MEMORY_AND_DISK)
      
      //do count on each rdd to enforce caching
//      uAxioms.count()
//      uAxiomsFlipped.count()
//      type1Axioms.count()
//      type2Axioms.count()
//      type2AxiomsMap1.count()
//      type2AxiomsMap2.count()
//      type3Axioms.count()
//      type4Axioms.count()
//      type5Axioms.count()
//      type6Axioms.count()
      

    //return the initialized RDDs as a Tuple object (can have at max 22 elements in Spark Tuple)
    (uAxioms, uAxiomsFlipped, rAxioms, type1Axioms, type2Axioms, type2AxiomsMap1, type2AxiomsMap2, type3Axioms, type4Axioms, type5Axioms, type6Axioms)
  }

  //completion rule1
  def completionRule1(uAxioms: RDD[(Int, Int)], type1Axioms: RDD[(Int, Int)], loopCounter: Int): RDD[(Int, Int)] = {

    val r1Join = type1Axioms.join(uAxioms)
                            .values
                            .distinct(8)
                            .partitionBy(hashPartitioner)
                           
    // uAxioms is immutable as it is input parameter, so use new constant uAxiomsNew
//    val uAxiomsNew = uAxioms.union(r1Join)   //union is partitioner aware
//                            .setName("uAxiomsRule1_"+loopCounter)
//                           .persist()
    r1Join
  }

  def completionRule2_deltaNew(loopCounter: Int, sc: SparkContext, type2A1A2: Broadcast[Set[(Int, Int)]], deltaUAxiomsFlipped: RDD[(Int, Int)], uAxiomsFlipped: RDD[(Int, Int)], type2AxiomsMap1: RDD[((Int, Int), Int)], type2AxiomsMap2: RDD[((Int, Int), Int)]): RDD[(Int, Int)] = {

    //flip the uAxioms for self join on subclass 
   // val uAxiomsFlipped = uAxioms.map({ case (a, x) => (x, a) }).partitionBy(hashPartitioner)

    //flip delta uAxioms
   // val deltaUAxiomsFlipped = deltaUAxioms.map({ case (a, x) => (x, a) }).partitionBy(hashPartitioner)

    //Compute flippedUAxioms by updating the init with the above deltaUAxiomsFlipped
   // val uAxiomsFlipped = uAxiomsFlippedInit.union(deltaUAxiomsFlipped)
    
    //JOIN 1
    val r2Join1 = uAxiomsFlipped.join(deltaUAxiomsFlipped)
                                .setName("r2Join1_"+loopCounter)

    //filter joined uaxioms result before remapping for second join
    val r2JoinFilter = r2Join1.filter{ case (x, (a1, a2)) => type2A1A2.value.contains((a1, a2)) || type2A1A2.value.contains((a2, a1)) } //need the flipped combination for delta
                                .setName("r2JoinFilter_"+loopCounter) 
    //JOIN 2 - PART 1
    val r2JoinFilterMap = r2JoinFilter.map({ case (x, (a1, a2)) => ((a1, a2), x) })
                                      .partitionBy(hashPartitioner)
                                      .setName("r2JoinFilterMap_"+loopCounter)
                                      .persist()
    
    //r2JoinFilterMap.count() //to force persist                                  
    
    // val type2AxiomsMap1 = type2Axioms.map({case(a1,(a2,b)) => ((a1,a2),b)}).partitionBy(type2Axioms.partitioner.get).persist()
    val r2Join21 = r2JoinFilterMap.join(type2AxiomsMap1).map({ case ((a1, a2), (x, b)) => (b, x) })
                                  .partitionBy(hashPartitioner)
                                  .setName("r2Join21_"+loopCounter)
    //JOIN 2 - PART 2
    
    // val type2AxiomsMap2 = type2Axioms.map({case(a1,(a2,b)) => ((a2,a1),b)}).partitionBy(type2Axioms.partitioner.get).persist()
    val r2Join22 = r2JoinFilterMap.join(type2AxiomsMap2)
                                  .map({ case ((a1, a2), (x, b)) => (b, x) })
                                  .partitionBy(hashPartitioner)
                                  .setName("r2Join22_"+loopCounter)
    //UNION join results
    val r2Join2 = r2Join21.union(r2Join22)

    
    //val uAxiomsNew = sc.union(uAxioms, r2Join21, r2Join22)
    //                   .setName("uAxiomsRule2_"+loopCounter)
   
    
     //unpersist intermediate results
     r2JoinFilterMap.unpersist()

    r2Join2

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

    var (uAxioms, uAxiomsFlipped, rAxioms, type1Axioms, type2Axioms, type2AxiomsMap1, type2AxiomsMap2, type3Axioms,
      type4Axioms, type5Axioms, type6Axioms) = initializeRDD(sc, dirPath)
      
     //  Thread.sleep(30000) //sleep for a minute  

    println("Before closure computation. Initial uAxioms count: " + uAxioms.count + ", Initial rAxioms count: " + rAxioms.count)

    var loopCounter: Int = 0

    var uAxiomsFinal = uAxioms
    var rAxiomsFinal = rAxioms

    var currDeltaURule1: RDD[(Int, Int)] = sc.emptyRDD
    var currDeltaURule2: RDD[(Int, Int)] = sc.emptyRDD
    var currDeltaURule4: RDD[(Int, Int)] = sc.emptyRDD
    var prevDeltaURule1: RDD[(Int, Int)] = sc.emptyRDD
    var prevDeltaURule2: RDD[(Int, Int)] = sc.emptyRDD
    var prevDeltaURule4: RDD[(Int, Int)] = sc.emptyRDD
    var prevUAxiomsFinal= uAxioms
    var prevRAxiomsFinal = rAxioms
    

    //for pre-filtering for rule2 - should some of this move to initRDD()?
    val type2Collect = type2Axioms.collect()
    val type2FillersA1A2 = type2Collect.map({ case (a1, (a2, b)) => (a1, a2) }).toSet
    val type2FillersBroadcast = sc.broadcast(type2FillersA1A2)

    while (loopCounter <= 10) {

      loopCounter += 1

      //Rule 1
      var t_begin_rule = System.nanoTime()
      var currDeltaURule1 = completionRule1(uAxiomsFinal, type1Axioms,loopCounter)
      currDeltaURule1 = currDeltaURule1.setName("deltaURule1_"+loopCounter).persist(StorageLevel.MEMORY_AND_DISK)
     // currDeltaURule1.count() // to force persist()
      var t_end_rule = System.nanoTime()
      println("----Completed rule1---- : ")
      // println("count: "+ uAxiomRule1Count+" Time taken: "+ (t_end_rule - t_begin_rule) / 1e6 + " ms")
      println("=====================================")

      
      
      //Prepare input to Rule2      
     // currDeltaURule1 = uAxiomsRule1.subtract(uAxiomsFinal)
     //                               .setName("currDeltaURule1_"+loopCounter)
      
      var uAxiomsRule1 = uAxiomsFinal.union(currDeltaURule1)
                                     .setName("uAxiomsRule1_"+loopCounter)
  //                                   .persist()
     
      val deltaUAxiomsForRule2 = {
        if (loopCounter == 1)
          currDeltaURule1
        else
          //sc.union(prevDeltaURule2, prevDeltaURule4, currDeltaURule1)
          sc.union(prevDeltaURule2, currDeltaURule1)
            .partitionBy(hashPartitioner) //if rule4 is not yet implemented, do not include prevDeltaURule4 in union
            .setName("deltaUAxiomsForRule2_"+loopCounter)
      }
      
      
      //flip delta uAxioms
      val deltaUAxiomsFlipped = deltaUAxiomsForRule2.map({ case (a, x) => (x, a) })   
      //update uAxiomsFlipped
      uAxiomsFlipped = sc.union(uAxiomsFlipped,deltaUAxiomsFlipped)
                         .distinct(8)
                         .partitionBy(hashPartitioner) 
                         .setName("uAxiomsFlipped_"+loopCounter)
//                         .persist(StorageLevel.MEMORY_AND_DISK)
       
                    
      //End of Prepare input to Rule2 
                                                                              
                                                                              
      //execute Rule 2
      t_begin_rule = System.nanoTime()
      var currDeltaURule2 = completionRule2_deltaNew(loopCounter, sc, type2FillersBroadcast, deltaUAxiomsForRule2, uAxiomsFlipped, type2AxiomsMap1, type2AxiomsMap2)
      //currDeltaURule2 = currDeltaURule2.setName("deltaURule2_"+loopCounter).persist(StorageLevel.MEMORY_AND_DISK)
      t_end_rule = System.nanoTime()
      println("----Completed rule2----")
      //println("count: "+ uAxiomRule2Count+" Time taken: "+ (t_end_rule - t_begin_rule) / 1e6 + " ms")
      println("=====================================")

      //compute deltaU after rule 2 to use it in the next iteration
     // currDeltaURule2 = uAxiomsRule2.subtract(uAxiomsRule1)
     //                               .partitionBy(hashPartitioner)
     //                               .setName("currDeltaURule2"+loopCounter)

       var uAxiomsRule2 = uAxiomsRule1.union(currDeltaURule2)
                                      .setName("uAxiomsRule2_"+loopCounter)                             
  //                                    .persist()
                                      
      // println("Partitioner for uAxiomsRule2: "+ uAxiomsRule2.partitioner)                               
      //TODO: update to the last rule you are testing
      //finalUAxiom assignment for use in next iteration 
      uAxiomsFinal = uAxiomsRule2
      
      uAxiomsFinal = uAxiomsFinal.distinct(8)
                                 .partitionBy(hashPartitioner)
                                 .setName("uAxiomsFinal_"+loopCounter)
                                 .persist(StorageLevel.MEMORY_AND_DISK)
     
    //  println("Partitioner for uAxiomsFinal: "+ uAxiomsFinal.partitioner)                           
      //prev RDD assignments
    //  prevUAxiomsFinal.unpersist()
    //   prevUAxiomsFinal = uAxiomsFinal
//      prevDeltaURule1.unpersist()
//      prevDeltaURule1 = currDeltaURule1
//      prevDeltaURule2.unpersist()                                      
//      prevDeltaURule2 = currDeltaURule2                             
     
      var t_begin_uAxiomCount = System.nanoTime()
      val currUAxiomsCount = uAxiomsFinal.count()
      var t_end_uAxiomCount = System.nanoTime()
      println("------Completed uAxioms count at the end of the loop: " + loopCounter + "--------")
      println("uAxiomCount: " + currUAxiomsCount + ", Time taken for uAxiom count: " + (t_end_uAxiomCount - t_begin_uAxiomCount) / 1e9 + " s")
      println("====================================")

    }
   
     val t_end = System.nanoTime()
     println("Total time taken for the program: "+ (t_end - t_init)/ 1e9 + " s")
     
     Thread.sleep(3000000) // add 100s delay for UI vizualization

    sc.stop()

  }

}