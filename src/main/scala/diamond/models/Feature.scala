package diamond.models

import diamond.models.AttributeType.AttributeType

/**
  * Feature metadata
  *
  * @param attribute String attribute name
  * @param attributeType AttributeType enum of Base, Transformed, Derived
  * @param namespace String family of related features
  * @param encoding String data type
  * @param description String
  * @param active Boolean
  *
  * Created by markmo on 12/12/2015.
  */
case class Feature(attribute: String,
                   attributeType: AttributeType,
                   namespace: String,
                   encoding: String,
                   description: String,
                   active: Boolean
                  ) {

  def toArray = Array(attribute, attributeType, namespace, encoding, description, active.toString)

}

object Feature {

  def fromArray(a: Array[String]) = Feature(a(0), AttributeType.withName(a(1)), a(2), a(3), a(4), a(5).toBoolean)

}

/**
  * Base attribute - from source data, e.g. 'Birth Date'
  * Transformed attribute - from transformation, e.g. Age
  * Derived attribute - output variable from model
  */
object AttributeType extends Enumeration {

  type AttributeType = Value

  val Base, Transformed, Derived = Value

}