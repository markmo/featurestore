package common.inference

/**
  * Created by markmo on 13/03/2016.
  */
object ValueType extends Enumeration {

  type ValueType = Value

  val None, Integer, Numeric, String, Date, Boolean, Bit, Text, Object, Array = Value

}