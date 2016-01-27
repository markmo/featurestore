package diamond.transform

import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types.DataType

import scala.language.implicitConversions

/**
  * All this is WIP - NOT READY
  *
  * Created by markmo on 19/12/2015.
  */

case class ResourceType(name: String)

class Task(name: String, hours: Int, resourceType: ResourceType) {

  def wrapHours(h: Int) = new {
    def hours = h
  }

  def wrapTask(name: String) = new Tuple1[String](name) {

    def needs(h: Int) = new Tuple2[String, Int](name, h) {

      def of(resourceType: ResourceType) = new Task(name, h, resourceType)

    }

  }

}


//val developer = new ResourceType("developer")
//val task = "feature #1" needs 32.h of developer


trait GivenElement

case class given(df: DataFrame, ctx: TransformationContext)(ts: GivenElement)

trait RowsElement

case class rows(elements: RowsElement*) extends GivenElement

trait TransformElement

class transform(name: String)(op: => Unit) extends RowsElement {

  def />(t: RowsElement) = this

}

object transform {

  def apply(name: String)(op: => Unit) = new transform(name)(op)

}

trait AppendElement

class append(name: String, columnName: String, dataType: DataType, nullable: Boolean = true)(op: => Unit) extends RowsElement {

  def />(t: RowsElement) = this

}

object append {

  def apply(name: String, columnName: String, dataType: DataType, nullable: Boolean = true)(op: => Unit) =
    new append(name, columnName, dataType, nullable)(op)

}
