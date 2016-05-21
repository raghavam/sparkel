package org.daselab.sparkel

import collection.JavaConverters._
import java.io.File
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.model.OWLOntologyManager
import org.semanticweb.owlapi.model.OWLOntology
import org.semanticweb.owlapi.model.OWLLogicalAxiom
import org.semanticweb.owlapi.formats.FunctionalSyntaxDocumentFormat
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom
import org.semanticweb.owlapi.model.OWLSubObjectPropertyOfAxiom
import org.semanticweb.owlapi.model.OWLSubPropertyChainOfAxiom
import org.semanticweb.owlapi.model.OWLObjectPropertyDomainAxiom
import org.semanticweb.owlapi.model.OWLClass
import org.semanticweb.owlapi.model.OWLObjectHasValue
import org.semanticweb.owlapi.model.OWLObjectSomeValuesFrom
import org.semanticweb.owlapi.model.OWLObjectIntersectionOf
import org.semanticweb.owlapi.model.OWLObjectOneOf

/**
 * Given an ontology or a set of ontology files, it checks and retains the axioms 
 * that belong to the description logic EL+. Rest of the axioms are dropped. 
 * If a set of ontology files are provided, they are merged. This is
 * specifically useful for the Traffic ontologies.
 */
object ELProfileAxiomRetainer {
  
  // indicates the number of axioms that have been ignored
  private var numAxiomsSkipped = 0
  
  def checkAndRetainAllowedAxioms(inputFiles: Array[File], 
      outputFilePath: String): Unit = {
    val ontologyManager = OWLManager.createOWLOntologyManager()
    val outputOWLFile = new File(outputFilePath)
    val outputOWLIRI = IRI.create(outputOWLFile)
    val outputOntology = ontologyManager.createOntology(outputOWLIRI)
    inputFiles.foreach(owlFile => checkAllowedAxiomsAndMerge(owlFile, 
        ontologyManager, outputOntology))
    println("Total axioms: " + outputOntology.getLogicalAxiomCount())
    println("Number of skipped axioms: " + numAxiomsSkipped)
    ontologyManager.saveOntology(outputOntology, 
        new FunctionalSyntaxDocumentFormat(), outputOWLIRI)
  }
  
  private def checkAllowedAxiomsAndMerge(owlFile: File, 
      ontologyManager: OWLOntologyManager, 
      outputOntology: OWLOntology): Unit = {
    val documentIRI = IRI.create(owlFile)
    val ontology = ontologyManager.loadOntologyFromOntologyDocument(documentIRI)
    ontology.getLogicalAxioms().asScala.foreach(axiom => {
      val axiomAllowed = isAxiomAllowed(axiom)
      if (axiomAllowed)
        ontologyManager.addAxiom(outputOntology, axiom) // merge it
      else 
        numAxiomsSkipped += 1
      })
    ontologyManager.removeOntology(ontology)  
  }
  
  private def isAxiomAllowed(axiom: OWLLogicalAxiom): Boolean = {
    var axiomAllowed = true
    axiom match {
      case subClassAxiom: OWLSubClassOfAxiom =>
        axiomAllowed = checkSubClassAxiom(subClassAxiom)
      case subObjectPropertyAxiom: OWLSubObjectPropertyOfAxiom =>
        axiomAllowed = true
      case subPropertyChainAxiom: OWLSubPropertyChainOfAxiom =>
        axiomAllowed = true
      case objectPropertyDomainAxiom: OWLObjectPropertyDomainAxiom => 
        axiomAllowed = checkObjectPropertyDomainAxiom(objectPropertyDomainAxiom)
      case _ => axiomAllowed = false
    }
    axiomAllowed
  }
  
  private def checkSubClassAxiom(subClassAxiom: OWLSubClassOfAxiom): Boolean = {
    var axiomAllowed = true
    val subClassExpression = subClassAxiom.getSubClass()
    val superClassExpression = subClassAxiom.getSuperClass()
    subClassExpression match {
      case leftAtomicConcept: OWLClass => superClassExpression match {
        case rightAtomicConcept: OWLClass => 
            // This is type1 axiom; A < B 
            axiomAllowed = true
        case objectHasValue: OWLObjectHasValue => 
            // This is of type A < 3r.{a}
            axiomAllowed = true
        case rightExistential: OWLObjectSomeValuesFrom =>
            // This is type3 axiom; A < 3r.B
            axiomAllowed = true
        case _ => axiomAllowed = false
      }
      case leftExistential: OWLObjectSomeValuesFrom => superClassExpression match {
        case rightAtomicConcept: OWLClass =>
            // This is type4 axiom; 3r.A < B
            axiomAllowed = true
        case _ => axiomAllowed = false
      }
      case objIntersection: OWLObjectIntersectionOf => superClassExpression match {
        case rightAtomicConcept: OWLClass => 
            // This is type2 axiom; A1 ^ A2 < B
            axiomAllowed = true
        case _ => axiomAllowed = false
      }
      case conceptNominal: OWLObjectOneOf => superClassExpression match {
        case rightAtomicConcept: OWLClass => 
          // expecting this to be of the form {a} < A
          val individuals = conceptNominal.getIndividuals().asScala
          if (individuals.size > 1)
            axiomAllowed = false
          else {
            axiomAllowed = true
          }  
        case _ => axiomAllowed = false
      }
      case _ => axiomAllowed = false
    }
    axiomAllowed
  }
   
  private def checkObjectPropertyDomainAxiom(
      objectPropertyDomainAxiom: OWLObjectPropertyDomainAxiom): Boolean = {
    // convert this to axiom of the form 3r.T < B
    checkSubClassAxiom(objectPropertyDomainAxiom.asOWLSubClassOfAxiom())
  }
  
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Please provide \n\t 1) the ontology file path or " + 
                "the directory containing ontologies " + 
	              "\n\t 2) output file path")
    } 
    else {
      val inputPath = new File(args(0))
      if (inputPath.isFile())
        checkAndRetainAllowedAxioms(Array(inputPath), args(1))
      else if (inputPath.isDirectory()) 
        checkAndRetainAllowedAxioms(inputPath.listFiles(), args(1))
    }
  }
}