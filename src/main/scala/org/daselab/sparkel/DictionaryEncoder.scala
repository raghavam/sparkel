package org.daselab.sparkel

import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.apibinding.OWLManager
import java.io.{File,PrintWriter,BufferedWriter,FileWriter}
import collection.JavaConverters._
import collection.mutable.{Map,Set}

/**
 * Encodes the axioms in the given ontology. Each string is mapped 
 * to an integer and the encoded axioms are written to files. Each
 * axiom type goes into one file.
 * 
 * @author Raghava Mutharaju
 */
object DictionaryEncoder {
  
  private var dictionary: Map[String, Int] = Map()
  
  def encodeAxioms(ontFilePath: String): Unit = {
    val ontology = loadOntology(ontFilePath)
    encodeOntologyTerms(ontology)
    //convert Java set to Scala set and apply the encoding function on each axiom
    ontology.getLogicalAxioms().asScala.foreach(encodeAxiomAndWriteToFile(_))
  }
  
  private def encodeOntologyTerms(ontology: OWLOntology): Unit = {
    val ontologyConcepts = ontology.getClassesInSignature().asScala
    val topConcept = ontology.getOWLOntologyManager().getOWLDataFactory().
                        getOWLThing().toString();
    var code: Int = 1
    dictionary += (topConcept -> code)
    code += 1
    ontologyConcepts.foreach((concept: OWLClass) => 
                       {dictionary += (concept.toString() -> code); code += 1})
    val ontologyProperties = ontology.getObjectPropertiesInSignature().asScala
    ontologyProperties.foreach((property: OWLObjectProperty) => 
                       {dictionary += (property.toString() -> code); code += 1})
    writeSAxiomsToFile(ontologyConcepts, topConcept)
    writeRAxiomsToFile(ontologyProperties)
  }
  
  private def writeSAxiomsToFile(ontologyConcepts: Set[OWLClass], topConcept: String): Unit = {
    val saxiomsWriter = new PrintWriter(new BufferedWriter(
                            new FileWriter("saxioms.txt")))
    val topConceptCode = dictionary.get(topConcept).toString()
    ontologyConcepts.foreach((concept: OWLClass) => {
        val conceptCode = dictionary.get(concept.toString()).toString(); 
        saxiomsWriter.println(conceptCode + "|" + conceptCode);
        saxiomsWriter.println(conceptCode + "|" + topConceptCode)})
    saxiomsWriter.close();
  }
  
  private def writeRAxiomsToFile(ontologyProperties: Set[OWLObjectProperty]): Unit = {
    
  }
  
  private def encodeAxiomAndWriteToFile(axiom: OWLLogicalAxiom): Unit = {
    axiom match {
      case subClassAxiom: OWLSubClassOfAxiom => 
                          handleSubClassAxiom(subClassAxiom)
      case subObjectPropertyAxiom: OWLSubObjectPropertyOfAxiom => 
                          handleSubObjectPropertyAxiom(subObjectPropertyAxiom)
      case subPropertyChainAxiom: OWLSubPropertyChainOfAxiom => 
                          handleSubPropertyChainAxiom(subPropertyChainAxiom)
      case _ => throwException(axiom)
    }
  }
  
  private def handleSubClassAxiom(subClassAxiom: OWLSubClassOfAxiom): Unit = {
    val subClassExpression = subClassAxiom.getSubClass()
    val superClassExpression = subClassAxiom.getSuperClass()
    subClassExpression match {
      case leftAtomicConcept: OWLClass => superClassExpression match {
          case rightAtomicConcept: OWLClass => 
          case rightExistential: OWLObjectSomeValuesFrom => 
          case _ => throwException(subClassAxiom)
          }
      case leftExistential: OWLObjectSomeValuesFrom => 
      case objIntersection: OWLObjectIntersectionOf => 
      case _ => throwException(subClassAxiom)
    }
  }
  
  private def handleSubObjectPropertyAxiom(subObjectPropertyAxiom: OWLSubObjectPropertyOfAxiom): Unit = {
    println(subObjectPropertyAxiom.toString() + "  subObjProp")
  }
  
  private def handleSubPropertyChainAxiom(subPropertyChainAxiom: OWLSubPropertyChainOfAxiom): Unit = {
    println(subPropertyChainAxiom.toString() + "  subPropChain")
  }
  
  private def throwException(axiom: OWLLogicalAxiom): Unit = {
    throw new Exception("Unexpected axiom type: " + axiom.toString())
  }
  
  private def loadOntology(ontFilePath: String): OWLOntology = {
    val owlFile = new File(ontFilePath)
    val documentIRI = IRI.create(owlFile)
    val manager = OWLManager.createOWLOntologyManager()
    manager.loadOntologyFromOntologyDocument(owlFile)
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