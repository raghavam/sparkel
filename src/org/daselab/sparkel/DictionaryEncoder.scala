package org.daselab.sparkel

import org.semanticweb.owlapi.model.{OWLOntologyManager,OWLOntology,IRI}
import org.semanticweb.owlapi.apibinding.OWLManager
import java.io.File
import collection.JavaConverters._

/**
 * Encodes the axioms in the given ontology. Each string is mapped 
 * to an integer and the encoded axioms are written to files. Each
 * axiom type goes into one file.
 * 
 * @author Raghava Mutharaju
 */
object DictionaryEncoder {
  
  def encodeAxioms(ontFilePath: String): Unit = {
    val ontology = loadOntology(ontFilePath)
    //convert Java set to Scala set and apply the encoding function on each axiom
    ontology.getLogicalAxioms().asScala.foreach(encodeAndWriteToFile(_))
  }
  
  private def encodeAndWriteToFile(axiom: OWLLogicalAxiom): Unit = {
    println(axiom.toString())
  }
  
  private def loadOntology(ontFilePath: String): OWLOntology = {
    val owlFile = new File(ontFilePath)
    val documentIRI = IRI.create(owlFile)
    val manager = OWLManager.createOWLOntologyManager()
    manager.loadOntologyFromOntologyDocument(documentIRI)
  }
  
  def main(args: Array[String]): Unit = {
    if(args.length != 1) {
      println("Please provide the ontology file path")
    }
    else {
      encodeAxioms(args(0))
    }
  }
}