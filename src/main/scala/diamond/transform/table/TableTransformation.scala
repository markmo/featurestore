package diamond.transform.table

import diamond.transform.{Transformation, TransformationContext}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
  * A general table-level transformation that takes a DataFrame and returns
  * a new DataFrame.
  *
  * The new DataFrame may conform to a different schema. It may be computed
  * with reference to the original DataFrame or to any values in the
  * TransformationContext.
  *
  * Created by markmo on 16/12/2015.
  */
trait TableTransformation extends Transformation {

  val dependencies = mutable.Set[TableTransformation]()

  def apply(df: DataFrame, ctx: TransformationContext): DataFrame

  def addDependencies(dependencies: TableTransformation*) {
    this.dependencies ++= dependencies
  }

  def edges: Traversable[(TableTransformation, TableTransformation)] = dependencies.map((_, this))

}
