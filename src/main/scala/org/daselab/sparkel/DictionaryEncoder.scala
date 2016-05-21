package org.daselab.sparkel

import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.apibinding.OWLManager
import java.io.{ File, PrintWriter, BufferedWriter, FileWriter }
import collection.JavaConverters._
import collection.mutable.{ Map, Set }
import argonaut._
import Argonaut._
import main.scala.org.daselab.sparkel.Constants._
import scala.collection.mutable

/**
 * Encodes the axioms in the given ontology. Each string is mapped
 * to an integer and the encoded axioms are written to files. Each
 * axiom type goes into one file.
 *
 * @author Raghava Mutharaju
 */
object DictionaryEncoder {

  private var dictionary: Map[String, Int] = Map()
  private var type1AxiomWriter, type2AxiomWriter: PrintWriter = _
  private var type3AxiomWriter, type4AxiomWriter: PrintWriter = _
  private var type5AxiomWriter, type6AxiomWriter: PrintWriter = _
  
  private var type1AxiomJsonWriter, type2AxiomJsonWriter: PrintWriter = _
  private var type3AxiomJsonWriter, type4AxiomJsonWriter: PrintWriter = _
  private var type5AxiomJsonWriter, type6AxiomJsonWriter: PrintWriter = _

  def encodeAxioms(ontFilePath: String): Unit = {
    val ontology = loadOntology(ontFilePath)
    encodeOntologyTerms(ontology)
    initializeWriters()
    //convert Java set to Scala set and apply the encoding function on each axiom
    ontology.getLogicalAxioms().asScala.foreach(encodeAxiomAndWriteToFile(_))
    closeWriters()
  }
  
  private def initializeWriters(): Unit = {
    type1AxiomWriter = new PrintWriter(new BufferedWriter(
        new FileWriter("Type1Axioms.txt")))
    type2AxiomWriter = new PrintWriter(new BufferedWriter(
        new FileWriter("Type2Axioms.txt")))
    type3AxiomWriter = new PrintWriter(new BufferedWriter(
        new FileWriter("Type3Axioms.txt")))
    type4AxiomWriter = new PrintWriter(new BufferedWriter(
        new FileWriter("Type4Axioms.txt")))
    type5AxiomWriter = new PrintWriter(new BufferedWriter(
        new FileWriter("Type5Axioms.txt")))
    type6AxiomWriter = new PrintWriter(new BufferedWriter(
        new FileWriter("Type6Axioms.txt")))
    
    type1AxiomJsonWriter = new PrintWriter(new BufferedWriter(
        new FileWriter("Type1Axioms.json")))
    type2AxiomJsonWriter = new PrintWriter(new BufferedWriter(
        new FileWriter("Type2Axioms.json")))
    type3AxiomJsonWriter = new PrintWriter(new BufferedWriter(
        new FileWriter("Type3Axioms.json")))
    type4AxiomJsonWriter = new PrintWriter(new BufferedWriter(
        new FileWriter("Type4Axioms.json")))
    type5AxiomJsonWriter = new PrintWriter(new BufferedWriter(
        new FileWriter("Type5Axioms.json")))
    type6AxiomJsonWriter = new PrintWriter(new BufferedWriter(
        new FileWriter("Type6Axioms.json")))
  }
  
  private def closeWriters(): Unit = {
    type1AxiomWriter.close()
    type2AxiomWriter.close()
    type3AxiomWriter.close()
    type4AxiomWriter.close()
    type5AxiomWriter.close()
    type6AxiomWriter.close()
    
    type1AxiomJsonWriter.close()
    type2AxiomJsonWriter.close()
    type3AxiomJsonWriter.close()
    type4AxiomJsonWriter.close()
    type5AxiomJsonWriter.close()
    type6AxiomJsonWriter.close()
  }

  private def encodeOntologyTerms(ontology: OWLOntology): Unit = {
    println("#Logical Axioms: " + ontology.getLogicalAxiomCount())
    val ontologyConcepts = ontology.getClassesInSignature().asScala
    println("#Concepts: " + ontologyConcepts.size)
    val topConcept = ontology.getOWLOntologyManager().getOWLDataFactory().
      getOWLThing().toString();
    var code: Int = 1
    dictionary += (topConcept -> code)
    code += 1
    ontologyConcepts.foreach((concept: OWLClass) =>
      { dictionary += (concept.toString() -> code); code += 1 })
      
    val ontologyProperties = ontology.getObjectPropertiesInSignature().asScala
    println("#Object Properties: " + ontologyProperties.size)
    ontologyProperties.foreach((property: OWLObjectProperty) =>
      { dictionary += (property.toString() -> code); code += 1 })
      
    val ontologyIndividuals = ontology.getIndividualsInSignature().asScala
    println("#Individuals: " + ontologyIndividuals.size)
    ontologyIndividuals.foreach(individual => 
      { dictionary += (individual.toString() -> code); code += 1 })
      
    writeSAxiomsToFile(ontologyConcepts, topConcept)
    writeDictionaryToFile()
  }

  /**
   * This function initializes S(X) set, i.e., S(X) = {X, T}. This is 
   * written as X|X, X|T in the file.
   */
  private def writeSAxiomsToFile(ontologyConcepts: Set[OWLClass], 
      topConcept: String): Unit = {
//    println("Writing S axioms to file ...")
    val saxiomsWriter = new PrintWriter(new BufferedWriter(
      new FileWriter("sAxioms.txt")))
    val saxiomsJsonWriter = new PrintWriter(new BufferedWriter(
      new FileWriter("sAxioms.json")))
    val topConceptCode = dictionary.get(topConcept).get
    ontologyConcepts.foreach((concept: OWLClass) => {
      val conceptCode = dictionary.get(concept.toString()).get;
      saxiomsWriter.println(conceptCode + TupleSeparator + conceptCode);
      saxiomsWriter.println(conceptCode + TupleSeparator + topConceptCode)    
      val conceptCodeJson = jNumber(conceptCode)
      val jsonObj1: Json = Json.obj(SAxiom.SubConcept -> conceptCodeJson, 
                                    SAxiom.SuperConcept -> conceptCodeJson)
      val jsonObj2: Json = Json.obj(SAxiom.SubConcept -> conceptCodeJson, 
                                    SAxiom.SuperConcept -> jNumber(topConceptCode))
      saxiomsJsonWriter.println(jsonObj1.toString())
      saxiomsJsonWriter.println(jsonObj2.toString())
    })
    saxiomsWriter.close()
    saxiomsJsonWriter.close()
  }
  
  private def writeDictionaryToFile(): Unit = {
    // reversing the dictionary, so that it is easier to lookup based on 
    // the code rather than the string
    val dictionaryWriter = new PrintWriter(new BufferedWriter(
      new FileWriter("dictionary.txt")))
    dictionary.foreach({case (k, v) => dictionaryWriter.println(
        v.toString() + TupleSeparator + k)})
    dictionaryWriter.close()
  }

  private def encodeAxiomAndWriteToFile(axiom: OWLLogicalAxiom): Unit = {
    axiom match {
      case subClassAxiom: OWLSubClassOfAxiom =>
        handleSubClassAxiom(subClassAxiom)
      case subObjectPropertyAxiom: OWLSubObjectPropertyOfAxiom =>
        handleSubObjectPropertyAxiom(subObjectPropertyAxiom)
      case subPropertyChainAxiom: OWLSubPropertyChainOfAxiom =>
        handleSubPropertyChainAxiom(subPropertyChainAxiom)
      case objectPropertyDomainAxiom: OWLObjectPropertyDomainAxiom => 
        handleObjectPropertyDomainAxiom(objectPropertyDomainAxiom)
      case _ => throwException(axiom)
    }
  }

  private def handleSubClassAxiom(subClassAxiom: OWLSubClassOfAxiom): Unit = {
    val subClassExpression = subClassAxiom.getSubClass()
    val superClassExpression = subClassAxiom.getSuperClass()
    subClassExpression match {
      case leftAtomicConcept: OWLClass => superClassExpression match {
        case rightAtomicConcept: OWLClass => 
            // This is type1 axiom; A < B 
            val leftConceptCode = dictionary.get(leftAtomicConcept.toString()).get
            val rightConceptCode = dictionary.get(rightAtomicConcept.toString()).get
            type1AxiomWriter.println(leftConceptCode + TupleSeparator + rightConceptCode)
            val type1JsonObj: Json = Json.obj(
                Type1Axiom.SubConcept -> jNumber(leftConceptCode), 
                Type1Axiom.SuperConcept -> jNumber(rightConceptCode))
            type1AxiomJsonWriter.println(type1JsonObj.toString())
        case objectHasValue: OWLObjectHasValue => 
            // This is of type A < 3r.{a}
            rightExistentialHelper(leftAtomicConcept, 
                objectHasValue.getProperty().toString(), 
                objectHasValue.getFiller().toString())
        case rightExistential: OWLObjectSomeValuesFrom =>
            // This is type3 axiom; A < 3r.B
            rightExistentialHelper(leftAtomicConcept, 
                rightExistential.getProperty().toString(), 
                rightExistential.getFiller().toString())
        case _ => throwException(subClassAxiom)
      }
      case leftExistential: OWLObjectSomeValuesFrom => superClassExpression match {
        case rightAtomicConcept: OWLClass =>
            // This is type4 axiom; 3r.A < B
            val propertyCode = dictionary.get(leftExistential.getProperty().toString()).get
            val leftConceptCode = dictionary.get(leftExistential.getFiller().toString()).get
            val rightConceptCode = dictionary.get(rightAtomicConcept.toString()).get
            type4AxiomWriter.println(propertyCode + TupleSeparator + 
                leftConceptCode + TupleSeparator + rightConceptCode)
            val type4JsonObj: Json = Json.obj(
                Type4Axiom.LHSRole -> jNumber(propertyCode), 
                Type4Axiom.LHSFiller -> jNumber(leftConceptCode), 
                Type4Axiom.SuperConcept -> jNumber(rightConceptCode))
            type4AxiomJsonWriter.println(type4JsonObj.toString())
        case _ => throwException(subClassAxiom)
      }
      case objIntersection: OWLObjectIntersectionOf => superClassExpression match {
        case rightAtomicConcept: OWLClass => 
            // This is type2 axiom; A1 ^ A2 < B
            var operandStr = new StringBuilder()
            val operands = objIntersection.getOperands().asScala
            // head and last operations work here because there are only 
            // 2 elements in the set
            val operand1Code = dictionary.get(operands.head.toString()).get
            val operand2Code = dictionary.get(operands.last.toString()).get
            operandStr.append(operand1Code).append(TupleSeparator).
                       append(operand2Code)
            val rightConceptCode = dictionary.get(rightAtomicConcept.toString()).get
            type2AxiomWriter.println(operandStr.toString() + TupleSeparator + 
                rightConceptCode)
            val type2JsonObj: Json = Json.obj(
                Type2Axiom.LeftConjunct1 -> jNumber(operand1Code), 
                Type2Axiom.LeftConjunct2 -> jNumber(operand2Code),
                Type2Axiom.SuperConcept -> jNumber(rightConceptCode))
            type2AxiomJsonWriter.println(type2JsonObj.toString())
        case _ => throwException(subClassAxiom)
      }
      case conceptNominal: OWLObjectOneOf => superClassExpression match {
        case rightAtomicConcept: OWLClass => 
          // expecting this to be of the form {a} < A
          val individuals = conceptNominal.getIndividuals().asScala
          if (individuals.size > 1)
            throwException(subClassAxiom)
          else {
            val leftConceptCode = dictionary.get(individuals.head.toString()).get
            val rightConceptCode = dictionary.get(rightAtomicConcept.toString()).get
            type1AxiomWriter.println(leftConceptCode + TupleSeparator + rightConceptCode)
            val type1JsonObj: Json = Json.obj(
                Type1Axiom.SubConcept -> jNumber(leftConceptCode), 
                Type1Axiom.SuperConcept -> jNumber(rightConceptCode))
            type1AxiomJsonWriter.println(type1JsonObj.toString())
          }  
        case _ => throwException(subClassAxiom)
      }
      case _ => throwException(subClassAxiom)
    }
  }

  private def rightExistentialHelper(leftAtomicConcept: OWLClass, 
      property: String, filler: String): Unit = {
    val leftConceptCode = dictionary.get(leftAtomicConcept.toString()).get
            val propertyCode = dictionary.get(property).get
            val rightConceptCode = dictionary.get(filler).get
            type3AxiomWriter.println(leftConceptCode + TupleSeparator + 
                propertyCode + TupleSeparator + rightConceptCode)
            val type3JsonObj: Json = Json.obj(
                Type3Axiom.SubConcept -> jNumber(leftConceptCode), 
                Type3Axiom.RHSRole -> jNumber(propertyCode),
                Type3Axiom.RHSFiller -> jNumber(rightConceptCode))
            type3AxiomJsonWriter.println(type3JsonObj.toString())
  }
  
  private def handleSubObjectPropertyAxiom(
      subObjectPropertyAxiom: OWLSubObjectPropertyOfAxiom): Unit = {
    val subPropertyCode = dictionary.get(
        subObjectPropertyAxiom.getSubProperty.toString()).get
    val superPropertyCode = dictionary.get(
        subObjectPropertyAxiom.getSuperProperty().toString()).get
    type5AxiomWriter.println(subPropertyCode + TupleSeparator + superPropertyCode)
    val type5JsonObj: Json = Json.obj(
        Type5Axiom.SubRole -> jNumber(subPropertyCode), 
        Type5Axiom.SuperRole -> jNumber(superPropertyCode))
    type5AxiomJsonWriter.println(type5JsonObj.toString())
  }

  private def handleSubPropertyChainAxiom(
      subPropertyChainAxiom: OWLSubPropertyChainOfAxiom): Unit = {
    val propertyChainStr = new StringBuilder()
    val propertyChain = subPropertyChainAxiom.getPropertyChain().asScala
    // head and last operations work here because there are only 
    // 2 elements in the set
    val property1Code = dictionary.get(propertyChain.head.toString()).get
    val property2Code = dictionary.get(propertyChain.last.toString()).get
    propertyChainStr.append(property1Code).append(TupleSeparator).
                     append(property2Code)
    val superPropertyCode = dictionary.get(
        subPropertyChainAxiom.getSuperProperty().toString()).get   
    type6AxiomWriter.println(propertyChainStr.toString() + TupleSeparator + 
        superPropertyCode)
    val type6JsonObj: Json = Json.obj(
        Type6Axiom.LHSRole1 -> jNumber(property1Code), 
        Type6Axiom.LHSRole2 -> jNumber(property2Code), 
        Type6Axiom.SuperRole -> jNumber(superPropertyCode))
    type6AxiomJsonWriter.println(type6JsonObj.toString())    
  }
  
  private def handleObjectPropertyDomainAxiom(
      objectPropertyDomainAxiom: OWLObjectPropertyDomainAxiom): Unit = {
    // convert this to axiom of the form 3r.T < B
    handleSubClassAxiom(objectPropertyDomainAxiom.asOWLSubClassOfAxiom())
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
    if (args.length != 1) {
      println("Please provide the ontology file path")
    } else {
      encodeAxioms(args(0))
    }
  }
}
