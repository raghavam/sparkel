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
import org.apache.spark.broadcast.Broadcast

object SparkELDAGAnalysis {
  
  var numPartitions = -1 // later insitialized from commandline
  
   /*
   * Initializes all the RDDs corresponding to each axiom-type. 
   */
  def initializeRDD(sc: SparkContext, dirPath: String) = {
  
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

    //return the initialized RDDs as a Tuple object (can have at max 22 elements in Spark Tuple)
    (uAxioms, rAxioms, type1Axioms, type2Axioms, type3Axioms, type4Axioms, type5Axioms, type6Axioms)
  }

  
  //completion rule1
  def completionRule1(uAxioms: RDD[(Int, Int)], type1Axioms: RDD[(Int, Int)]): RDD[(Int, Int)] = {

    val r1Join = type1Axioms.join(uAxioms, numPartitions).map({ case (k, v) => v })
    // uAxioms is immutable as it is input parameter, so use new constant uAxiomsNew
    val uAxiomsNew = uAxioms.union(r1Join).distinct.partitionBy(uAxioms.partitioner.get).persist() 
    uAxiomsNew
  }
  
  def completionRule2_deltaNew(type2A1A2: Set[(Int, Int)], deltaUAxioms: RDD[(Int, Int)], uAxioms: RDD[(Int, Int)], type2Axioms: RDD[(Int, (Int, Int))]): RDD[(Int, Int)] = {
 
    
    //flip the uAxioms for self join on subclass 
    val uAxiomsFlipped = uAxioms.map({case (a,x) => (x,a)}).partitionBy(type2Axioms.partitioner.get)
    
    //for delta version
    val deltaUAxiomsFlipped = deltaUAxioms.map({case (a,x) => (x,a)})
    
    //JOIN 1
    val r2Join1 = uAxiomsFlipped.join(deltaUAxiomsFlipped).partitionBy(type2Axioms.partitioner.get)
    
    
    //filter joined uaxioms result before remapping for second join
    val r2JoinFilter = r2Join1.filter{ case (x, (a1,a2)) => type2A1A2.contains((a1,a2)) || type2A1A2.contains((a2,a1)) } //need the flipped combination for delta
      
    //JOIN 2 - PART 1
    val r2JoinFilterMap = r2JoinFilter.map({case (x, (a1,a2)) => ((a1,a2),x)}).partitionBy(type2Axioms.partitioner.get)
    //TODO could be saved in initRDD instead
    val type2AxiomsMap1 = type2Axioms.map({case(a1,(a2,b)) => ((a1,a2),b)}).partitionBy(type2Axioms.partitioner.get).persist()
    val r2Join21 = r2JoinFilterMap.join(type2AxiomsMap1).map({case ((a1,a2),(x,b)) => (b,x)}).partitionBy(type2Axioms.partitioner.get)
    
        
    //JOIN 2 - PART 2
    //TODO could be saved in initRDD instead
    val type2AxiomsMap2 = type2Axioms.map({case(a1,(a2,b)) => ((a2,a1),b)}).partitionBy(type2Axioms.partitioner.get).persist()
    val r2Join22 = r2JoinFilterMap.join(type2AxiomsMap2).map({case ((a1,a2),(x,b)) => (b,x)}).partitionBy(type2Axioms.partitioner.get)
    
    //UNION join results
    val r2Join2 = r2Join21.union(r2Join22)
    
//    
//    //union with uAxioms
    val uAxiomsNew = uAxioms.union(r2Join2).distinct.partitionBy(type2Axioms.partitioner.get)   
    
    uAxiomsNew

  }
  
  
   //Deletes the exisiting output directory
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
    if (args.length != 4) {
      System.err.println("Missing args:\n\t 0. input directory containing the axiom files \n\t" +
        "1. output directory to save the final computed sAxioms \n\t 2. Number of worker nodes in the cluster \n\t"+
        "3. Number of partitions (initial)")
      System.exit(-1)
    }
    
    val dirDeleted = deleteDir(args(1))
    numPartitions = args(3).toInt

    //init time
    val t_init = System.nanoTime()
    
    val conf = new SparkConf().setAppName("SparkEL")
    val sc = new SparkContext(conf)
    
    var (uAxioms, rAxioms, type1Axioms, type2Axioms, type3Axioms, 
        type4Axioms, type5Axioms, type6Axioms) = initializeRDD(sc, args(0))     
        
    println("Before closure computation. Initial uAxioms count: " + uAxioms.count + ", Initial rAxioms count: " + rAxioms.count)
    
    var loopCounter: Int = 0
    
    var uAxiomsFinal = uAxioms
    var rAxiomsFinal = rAxioms
    
    var currDeltaURule1: RDD[(Int, Int)] = sc.emptyRDD
    var currDeltaURule2: RDD[(Int, Int)] = sc.emptyRDD
    var prevDeltaURule2: RDD[(Int, Int)] = sc.emptyRDD
    var prevDeltaURule4: RDD[(Int, Int)] = sc.emptyRDD
    var currDeltaURule4: RDD[(Int, Int)] = sc.emptyRDD
    
    //for pre-filtering for rule2 - should some of this move to initRDD()?
    val type2Collect = type2Axioms.collect()
    val type2FillersA1A2 = type2Collect.map({ case (a1,(a2,b)) => (a1,a2)}).toSet
    val type2FillersBroadcast = sc.broadcast(type2FillersA1A2)
    
    while(loopCounter <= 10){
      
     loopCounter +=1
     
     
      //Rule 1
      var t_begin_rule = System.nanoTime()
      var uAxiomsRule1 = completionRule1(uAxiomsFinal, type1Axioms)
      var uAxiomRule1Count = uAxiomsRule1.count
      var t_end_rule = System.nanoTime()      
      println("----Completed rule1---- : ")
      println("count: "+ uAxiomRule1Count+" Time taken: "+ (t_end_rule - t_begin_rule) / 1e6 + " ms")
      println("=====================================")
      
            
      
      //Prepare input to Rule2      
      currDeltaURule1 = uAxiomsRule1.subtract(uAxiomsFinal).partitionBy(type2Axioms.partitioner.get).cache()
      val deltaUAxiomsForRule2 = { 
         if (loopCounter == 1)
           currDeltaURule1 
         else
           sc.union(prevDeltaURule2, prevDeltaURule4, currDeltaURule1).distinct.partitionBy(type2Axioms.partitioner.get)   
      }
     
    
      //execute Rule 2
      t_begin_rule = System.nanoTime()
      var uAxiomsRule2 = completionRule2_deltaNew(type2FillersA1A2,deltaUAxiomsForRule2,uAxiomsRule1,type2Axioms)
      var uAxiomRule2Count = uAxiomsRule2.count
      t_end_rule = System.nanoTime() 
      println("----Completed rule2----")
      //println("count: "+ uAxiomRule2Count+" Time taken: "+ (t_end_rule - t_begin_rule) / 1e6 + " ms")
      println("=====================================")
      
      //compute deltaU after rule 2 to use it in the next iteration
      currDeltaURule2 = uAxiomsRule2.subtract(uAxiomsRule1).partitionBy(type2Axioms.partitioner.get)
      
      //finalUAxiom assignment for use in next iteration 
      uAxiomsFinal = uAxiomsRule2
      
      //prev RDD assignments
      prevDeltaURule2 = currDeltaURule2 // should this be val?
      prevDeltaURule4 = currDeltaURule4 // should this be val
      
      var t_begin_uAxiomCount = System.nanoTime() 
      val currUAxiomsCount = uAxiomsFinal.count()
      var t_end_uAxiomCount = System.nanoTime()
      println("------Completed uAxioms count at the end of the loop: "+loopCounter+"--------")
      println("uAxiomCount: "+currUAxiomsCount+", Time taken for uAxiom count: "+ (t_end_uAxiomCount - t_begin_uAxiomCount) / 1e6 + " ms")
      println("====================================")
   
      
    }
    
    Thread.sleep(100000) // add 10s delay for UI vizualization
    
    sc.stop()
    
    
  }
  
}