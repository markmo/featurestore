package diamond.transformation.row

import diamond.transformation.{Transformation, TransformationContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
  * A general row transformation that takes a Row and returns a new Row.
  *
  * The new Row may conform to a different schema. The new Row may be
  * computed with reference to any values in the original Row or to any
  * values in the TransformationContext.
  *
  * Created by markmo on 12/12/2015.
  */
trait RowTransformation extends Transformation {

  val dependencies = mutable.Set[RowTransformation]()

  def apply(row: Row, ctx: TransformationContext): Row

  def addDependencies(dependencies: RowTransformation*) = {
    this.dependencies ++= dependencies
    this
  }

  def edges: Traversable[(RowTransformation, RowTransformation)] = dependencies.map((_, this))

}

/*
  * experimental feature only implemented from 2.11
  * http://stackoverflow.com/questions/25234682/in-scala-can-you-make-an-anonymous-function-have-a-default-argument
  *
trait TransformFunc {

  def apply(row: Row, ctx: TransformationContext, fieldLocator: (String => Any) = { _ => None }): Row

}*/

object RowTransformation {

  val SCHEMA_KEY = "schema"

  //def apply(name: String)(op: TransformFunc) =

  def apply(name: String)(op: (Row, TransformationContext) => Row) = {
    val myName = name
    new RowTransformation {

      val name = myName

      def apply(row: Row, ctx: TransformationContext): Row = op(row, ctx)

    }
  }

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
