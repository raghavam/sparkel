package org.daselab.sparkel.reasonertest

import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.apibinding.OWLManager
import java.io.{ File, PrintWriter, BufferedWriter, FileWriter }
import org.semanticweb.owlapi.reasoner.{ OWLReasoner, OWLReasonerFactory, InferenceType }
import collection.JavaConverters._
import org.semanticweb.elk.owlapi.ElkReasonerFactory
import scala.io.Source
import main.scala.org.daselab.sparkel.Constants._

object ReasonerDiffComparator {
  
  private var dictionary: Map[String, Int] = _
  private var outputWriter: PrintWriter = _
    
  /**
   * compares the output of SparkEL with an existing reasoner's output
   */
  def compareReasonerOutputs(ontFilePath: String): Unit = {
    val ontology = loadOntology(ontFilePath)
    val reasonerFactory = new ElkReasonerFactory()
		val reasoner = reasonerFactory.createReasoner(ontology)
		val ontologyConcepts = ontology.getClassesInSignature().asScala
		// get the reasoner output of each class and compare with SparkEL's output
  }
  
  def printReasonerOutput(ontFilePath: String, dictionaryFilePath: String): Unit = {
    val dictionarySource = Source.fromFile(dictionaryFilePath)
    for(line <- dictionarySource.getLines()) {
      line.split(TupleSeparator) match {case Array(x, y) => dictionary += (y -> x.toInt)}
    }
    dictionarySource.close()
    outputWriter = new PrintWriter(new BufferedWriter(
        new FileWriter("output.txt")))
    val ontology = loadOntology(ontFilePath)
    val reasonerFactory = new ElkReasonerFactory()
		val reasoner = reasonerFactory.createReasoner(ontology)
		reasoner.precomputeInferences(InferenceType.CLASS_HIERARCHY)
		val ontologyConcepts = ontology.getClassesInSignature().asScala
		ontologyConcepts.foreach(classHierarchy(_, reasoner))
		reasoner.dispose()
		outputWriter.close()
  }
  
  private def classHierarchy(concept: OWLClass, reasoner: OWLReasoner): Unit = {
    val superClasses = reasoner.getSuperClasses(concept, false).getFlattened.asScala
    val conceptCode = dictionary.get(concept.toString()).get
    superClasses.foreach((superConcept: OWLClass) => 
      outputWriter.println(conceptCode + TupleSeparator + 
          dictionary.get(superConcept.toString()).get))
  }
  
  private def loadOntology(ontFilePath: String): OWLOntology = {
    val owlFile = new File(ontFilePath)
    val documentIRI = IRI.create(owlFile)
    val manager = OWLManager.createOWLOntologyManager()
    manager.loadOntologyFromOntologyDocument(owlFile)
  }
  
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Please provide the ontology file path and dictionary.txt file")
    } else {
      printReasonerOutput(args(0), args(1))
    }
  }
}