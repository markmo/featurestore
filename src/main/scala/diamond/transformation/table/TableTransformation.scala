package diamond.transformation.table

import diamond.transformation.{Transformation, TransformationContext}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
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
