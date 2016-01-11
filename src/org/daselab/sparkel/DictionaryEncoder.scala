package org.daselab.sparkel

import org.semanticweb.owlapi.model.{OWLOntologyManager,OWLOntology};
import java.io.File;

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
  }
  
  private def loadOntology(ontFilePath: String): OWLOntology = {
    val owlFile = new File(ontFilePath)
    val documentIRI = IRI.create(owlFile)
    val manager = OWLManager.createOWLOntologyManager()
    manager.loadOntologyFromOntologyDocument(documentIRI)
  }
  
  def main(args: Array[String]): Unit = {
    
  }
}