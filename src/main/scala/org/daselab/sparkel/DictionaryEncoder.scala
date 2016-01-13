package org.daselab.sparkel

import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.apibinding.OWLManager
import java.io.{ File, PrintWriter, BufferedWriter, FileWriter }
import collection.JavaConverters._
import collection.mutable.{ Map, Set }
import main.scala.org.daselab.sparkel.Constants._

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

  def encodeAxioms(ontFilePath: String): Unit = {
    val ontology = loadOntology(ontFilePath)
    encodeOntologyTerms(ontology)
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
    //convert Java set to Scala set and apply the encoding function on each axiom
    ontology.getLogicalAxioms().asScala.foreach(encodeAxiomAndWriteToFile(_))
    type1AxiomWriter.close()
    type2AxiomWriter.close()
    type3AxiomWriter.close()
    type4AxiomWriter.close()
    type5AxiomWriter.close()
    type6AxiomWriter.close()
  }

  private def encodeOntologyTerms(ontology: OWLOntology): Unit = {
    val ontologyConcepts = ontology.getClassesInSignature().asScala
    val topConcept = ontology.getOWLOntologyManager().getOWLDataFactory().
      getOWLThing().toString();
    var code: Int = 1
    dictionary += (topConcept -> code)
    code += 1
    ontologyConcepts.foreach((concept: OWLClass) =>
      { dictionary += (concept.toString() -> code); code += 1 })
    val ontologyProperties = ontology.getObjectPropertiesInSignature().asScala
    ontologyProperties.foreach((property: OWLObjectProperty) =>
      { dictionary += (property.toString() -> code); code += 1 })
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
    val topConceptCode = dictionary.get(topConcept).get
    ontologyConcepts.foreach((concept: OWLClass) => {
      val conceptCode = dictionary.get(concept.toString()).get;
      saxiomsWriter.println(conceptCode + TupleSeparator + conceptCode);
      saxiomsWriter.println(conceptCode + TupleSeparator + topConceptCode)
    })
    saxiomsWriter.close();
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
        case rightExistential: OWLObjectSomeValuesFrom =>
            // This is type3 axiom; A < 3r.B
            val leftConceptCode = dictionary.get(leftAtomicConcept.toString()).get
            val propertyCode = dictionary.get(rightExistential.getProperty().toString()).get
            val rightConceptCode = dictionary.get(rightExistential.getFiller().toString()).get
            type3AxiomWriter.println(leftConceptCode + TupleSeparator + 
                propertyCode + TupleSeparator + rightConceptCode)
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
        case _ => throwException(subClassAxiom)
      }
      case objIntersection: OWLObjectIntersectionOf => superClassExpression match {
        case rightAtomicConcept: OWLClass => 
            // This is type2 axiom; A1 ^ A2 < B
            var operandStr = new StringBuilder()
            objIntersection.getOperands().asScala.foreach(
                (operand: OWLClassExpression) => {
                  val operandCode = dictionary.get(operand.toString()).get
                  operandStr.append(operandCode).append(TupleSeparator)
                })
            val rightConceptCode = dictionary.get(rightAtomicConcept.toString()).get
            // operandStr contains a '|' at the end, so no need to include it again
            type2AxiomWriter.println(operandStr.toString() + rightConceptCode)
        case _ => throwException(subClassAxiom)
      }
      case _ => throwException(subClassAxiom)
    }
  }

  private def handleSubObjectPropertyAxiom(
      subObjectPropertyAxiom: OWLSubObjectPropertyOfAxiom): Unit = {
    val subPropertyCode = dictionary.get(
        subObjectPropertyAxiom.getSubProperty.toString()).get
    val superPropertyCode = dictionary.get(
        subObjectPropertyAxiom.getSuperProperty().toString()).get
    type5AxiomWriter.println(subPropertyCode + TupleSeparator + superPropertyCode)
  }

  private def handleSubPropertyChainAxiom(
      subPropertyChainAxiom: OWLSubPropertyChainOfAxiom): Unit = {
    val propertyChainStr = new StringBuilder()
    subPropertyChainAxiom.getPropertyChain().asScala.foreach(
        (property: OWLObjectPropertyExpression) => {
          val propertyCode = dictionary.get(property.toString()).get
          propertyChainStr.append(propertyCode).append(TupleSeparator)
        })
    val superPropertyCode = dictionary.get(
        subPropertyChainAxiom.getSuperProperty().toString()).get
    // propertyChainStr contains a '|' at the end, so no need to include it again    
    type6AxiomWriter.println(propertyChainStr.toString() + superPropertyCode)
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