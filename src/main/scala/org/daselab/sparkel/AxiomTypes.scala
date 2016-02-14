package org.daselab.sparkel

/**
 * All the 6 different axiom types are represented here. Each case class 
 * has a companion object that holds the name of the fields in the case class.
 * This is useful if the field names have to be changed. 
 * 
 * @author Raghava Mutharaju
 */

/**
 * Represents axioms of type A < B. Using Long instead of Int since Spark 
 * when reading the encoded axioms in json format interprets them as BigInt
 * and throws an (upcast) exception when using Int.
 */
case class Type1Axiom(subConcept: Long, superConcept: Long)

object Type1Axiom {
  val SubConcept = "subConcept"
  val SuperConcept = "superConcept"
}

/**
 * Represents axioms of type A1 ^ A2 < B
 */
case class Type2Axiom(leftConjunct1: Long, leftConjunct2: Long, superConcept: Long)

object Type2Axiom {
  val LeftConjunct1 = "leftConjunct1"
  val LeftConjunct2 = "leftConjunct2"
  val SuperConcept = "superConcept"
}

/**
 * Represents axioms of type A < 3r.B
 */
case class Type3Axiom(subConcept: Long, rhsRole: Long, rhsFiller: Long)

object Type3Axiom {
  val SubConcept = "subConcept"
  val RHSRole = "rhsRole"
  val RHSFiller = "rhsFiller"
}

/**
 * Represents axioms of type 3r.A < B
 */
case class Type4Axiom(lhsRole: Long, lhsFiller: Long, superConcept: Long)

object Type4Axiom {
  val LHSRole = "lhsRole"
  val LHSFiller = "lhsFiller"
  val SuperConcept = "superConcept"
}

/**
 * Represents axioms of type r < s
 */
case class Type5Axiom(subRole: Long, superRole: Long)

object Type5Axiom {
  val SubRole = "subRole"
  val SuperRole = "superRole"
}

/**
 * Represents axioms of type r o s < t
 */
case class Type6Axiom(lhsRole1: Long, lhsRole2: Long, superRole: Long)

object Type6Axiom {
  val LHSRole1 = "lhsRole1"
  val LHSRole2 = "lhsRole2"
  val SuperRole = "superRole"
}

/**
 * Represents S(X) axioms. If S(X) = {A}, then X < Y
 */
case class SAxiom(subConcept: Long, superConcept: Long)

object SAxiom {
  val SubConcept = "subConcept"
  val SuperConcept = "superConcept"
}

/**
 * Represents R(r) = {(X, Y)} axioms
 */
case class RAxiom(role: Long, pairX: Long, pairY: Long)

object RAxiom {
  val Role = "role"
  val PairX = "pairX"
  val PairY = "pairY"
}


