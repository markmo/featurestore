package diamond.transformation

import org.apache.spark.sql.Row

/**
  * Created by markmo on 12/12/2015.
  */
trait AppendColumnTransformation extends Transformation {

  def apply(row: Row, ctx: TransformationContext): Row =
    Row.fromSeq(row.toSeq :+ append(row, ctx))

  def append(row: Row, ctx: TransformationContext): Any

}
