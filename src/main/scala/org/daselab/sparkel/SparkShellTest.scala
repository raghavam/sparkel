//package org.daselab.sparkel

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

//object SparkShellTest {
var numPartitions = -1 // later initialized from command line
  var hashPartitioner: HashPartitioner = null

  /*
   * Initializes all the RDDs corresponding to each axiom-type. 
   * Note, we want all the typeAxioms to reside in cache() and not on disk, 
   * hence we are using the default persistence level, i.e. Memory_Only.
   */
  def initializeRDD(sc: SparkContext, dirPath: String) = {
    
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

    //return the initialized RDDs as a Tuple object (can have at max 22 elements in Spark Tuple)
    (uAxioms, uAxiomsFlipped, rAxioms, type1Axioms, type2Axioms, type2AxiomsMap1, type2AxiomsMap2, type3Axioms, type4Axioms, type5Axioms, type6Axioms)
  }

  //completion rule1
  def completionRule1(uAxioms: RDD[(Int, Int)], type1Axioms: RDD[(Int, Int)], loopCounter: Int): RDD[(Int, Int)] = {

    val r1Join = type1Axioms.join(uAxioms)
                            .values
                            .distinct(8)
                            .partitionBy(hashPartitioner)
    r1Join
  }

  def completionRule2_deltaNew(loopCounter: Int, sc: SparkContext, type2A1A2: Broadcast[Set[(Int, Int)]], deltaUAxiomsFlipped: RDD[(Int, Int)], uAxiomsFlipped: RDD[(Int, Int)], type2AxiomsMap1: RDD[((Int, Int), Int)], type2AxiomsMap2: RDD[((Int, Int), Int)]): RDD[(Int, Int)] = {

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
    
    r2JoinFilterMap.count() //to force persist                                  
    
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
    
     //unpersist intermediate results
     r2JoinFilterMap.unpersist()

    r2Join2

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

  /*
   * The main method that initializes and calls each function corresponding to the completion rule 
   */
//  def main(args: Array[String]): Unit = {

    val dirDeleted = deleteDir("hdfs://10.0.0.5:8020/user/azureuser/sparkel/output/snomed/")
    numPartitions = 8
    hashPartitioner = new HashPartitioner(numPartitions)
    val dirPath = "hdfs://10.0.0.5:8020/user/azureuser/sparkel/ontologies/snomed/1/"

    //init time
    val t_init = System.nanoTime()

//    val conf = new SparkConf().setAppName("SparkEL")
//    val sc = new SparkContext(conf)

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
//      currDeltaURule1 = currDeltaURule1.setName("deltaURule1_"+loopCounter)
//                                       .persist(StorageLevel.MEMORY_AND_DISK)
//                                       .count() // to force persist()
      var t_end_rule = System.nanoTime()
      println("----Completed rule1---- : ")
      // println("count: "+ uAxiomRule1Count+" Time taken: "+ (t_end_rule - t_begin_rule) / 1e6 + " ms")
      println("=====================================")

      
      var uAxiomsRule1 = uAxiomsFinal.union(currDeltaURule1)
                                     .setName("uAxiomsRule1_"+loopCounter)
                                     .persist(StorageLevel.MEMORY_AND_DISK)
     
      uAxiomsFinal = uAxiomsRule1      
      uAxiomsFinal = uAxiomsFinal.distinct(8)
                                 .partitionBy(hashPartitioner)
                                 .setName("uAxiomsFinal_"+loopCounter)
                                 .persist(StorageLevel.MEMORY_AND_DISK)                      
     
      var t_begin_uAxiomCount = System.nanoTime()
      val currUAxiomsCount = uAxiomsFinal.count()
      var t_end_uAxiomCount = System.nanoTime()
      println("------Completed uAxioms count at the end of the loop: " + loopCounter + "--------")
      println("uAxiomCount: " + currUAxiomsCount + ", Time taken for uAxiom count: " + (t_end_uAxiomCount - t_begin_uAxiomCount) / 1e9 + " s")
      println("====================================")

    }
   
     val t_end = System.nanoTime()
     println("Total time taken for the program: "+ (t_end - t_init)/ 1e9 + " s")

//    sc.stop()
//    System.exit(0)
//  }

//}