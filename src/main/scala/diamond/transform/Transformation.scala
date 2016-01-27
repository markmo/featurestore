package diamond.transform

import scala.collection.mutable

/**
  * Created by markmo on 16/12/2015.
  */
trait Transformation extends Serializable {

  val name: String

  val dependencies: mutable.Set[_ <: Transformation]

  def edges: Traversable[(Transformation, Transformation)]

}
