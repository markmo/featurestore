package diamond.models

/**
  * Created by markmo on 12/12/2015.
  */
case class AttributeType(attribute: String,
                         namespace: String,
                         encoding: String,
                         description: String,
                         active: Boolean
                        ) {

  def toArray = Array(attribute, namespace, encoding, description, active.toString)

}

object AttributeType {

  def fromArray(a: Array[String]) = AttributeType(a(0), a(1), a(2), a(3), a(4).toBoolean)

}