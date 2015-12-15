package diamond.transformation

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable

import Transformation._

/**
  * Created by markmo on 12/12/2015.
  */
trait Transformation extends Serializable {

  val name: String

  val dependencies = mutable.Set[Transformation]()

  def apply(row: Row, ctx: TransformationContext): Row

  def addDependencies(dependencies: Transformation*) {
    this.dependencies ++= dependencies
  }

  def edges(): Traversable[(Transformation, Transformation)] = dependencies.map((_, this))

  /**
    * Given a row, returns a function that will lookup fields by name
    * and return the right type.
    *
    * Depends on the row schema being set in the context using
    * Transformation.SCHEMA_KEY
    *
    * @param row a spark.sql.Row
    * @param ctx a TransformationContext
    * @return a function
    */
  def fieldLocator(row: Row, ctx: TransformationContext) =
    if (ctx.contains(SCHEMA_KEY)) {
      val schema = ctx(SCHEMA_KEY).asInstanceOf[StructType]
      (name: String) => {
        val field = schema(name)
        val index = schema.fieldIndex(name)
        field.dataType match {
          case IntegerType => row.getInt(index)
          case DoubleType => row.getDouble(index)
          case BooleanType => row.getBoolean(index)
          case DateType => row.getDate(index)
          case TimestampType => row.getTimestamp(index)
          case _ => row.getString(index)
        }
      }
    } else {
      throw new RuntimeException("No schema in context")
    }

}

object Transformation {

  val SCHEMA_KEY = "schema"

}