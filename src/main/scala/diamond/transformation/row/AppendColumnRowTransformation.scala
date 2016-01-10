package diamond.transformation.row

import diamond.transformation.TransformationContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StringType, StructField}

/**
  * Appends a new column value to a row.
  *
  * Instead of implementing `apply`, the developer implements `append`,
  * which returns the new value. The value may be calculated with reference
  * to any other value in the Row or to the TransformationContext.
  *
  * Created by markmo on 12/12/2015.
  */
trait AppendColumnRowTransformation extends RowTransformation {

  // the name of the new column
  val columnName: String

  // the Spark SQL Type
  val dataType: DataType

  // can the column have no value?
  val nullable: Boolean

  // adds the new column to each row
  def apply(row: Row, ctx: TransformationContext): Row =
    Row.fromSeq(row.toSeq :+ append(row, ctx))

  // must be implemented by the developer
  def append(row: Row, ctx: TransformationContext): Any

  def meta = StructField(columnName, dataType, nullable)

}

object AppendColumnRowTransformation {

  // convenience to define a transformation instance
  def apply(name: String,
            columnName: String,
            dataType: DataType = StringType,
            nullable: Boolean = true
           )(op: (Row, TransformationContext) => Any) = {
    // otherwise conflicts with trait properties
    val myName = name
    val myColumnName = columnName
    val myDataType = dataType
    val isNullable = nullable

    new AppendColumnRowTransformation {

      val name = myName

      val columnName = myColumnName

      val dataType = myDataType

      val nullable = isNullable

      def append(row: Row, ctx: TransformationContext) = op(row, ctx)

    }
  }

}
