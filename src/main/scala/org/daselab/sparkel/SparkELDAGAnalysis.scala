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
    val uAxiomsNew = uAxioms.union(r1Join).distinct.partitionBy(uAxioms.partitioner.get) 
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
    
    //Rule 1
    var t_begin_rule = System.nanoTime()
    var uAxiomsRule1 = completionRule1(uAxioms, type1Axioms)
    var uAxiomRule1Count = uAxiomsRule1.count
    var t_end_rule = System.nanoTime()      
    println("----Completed rule1---- : ")
    println("count: "+ uAxiomRule1Count+" Time taken: "+ (t_end_rule - t_begin_rule) / 1e6 + " ms")
    println("=====================================")
    
    
    Thread.sleep(10000) // add 10s delay for UI vizualization
    
    sc.stop()
    
    
  }
  
}