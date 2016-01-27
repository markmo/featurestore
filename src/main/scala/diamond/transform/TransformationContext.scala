package diamond.transform

import diamond.models.{JobStep, TransformationError}

import scala.collection.mutable

/**
  *
  * Use this like any map to pass state between transformations if required.
  *
  * Created by markmo on 12/12/2015.
  */
class TransformationContext extends Serializable {

  val map = mutable.Map[String, Any]()

  apply("errors", List[TransformationError]())
  apply("steps", List[JobStep]())
  apply("sqlparams", Map[String, String]())

  def apply(key: String, value: Any) {
    map.put(key, value)
  }

  def apply(key: String) = map(key)

  def getOrElse(key: String, default: Any) = map.getOrElse(key, default)

  def contains(key: String) = map.contains(key)

}
