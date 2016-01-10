package diamond.transformation.table

import diamond.transformation.row.{AppendColumnRowTransformation, RowTransformation}
import diamond.transformation.utilityFunctions._
import diamond.transformation.{Pipeline, TransformationContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

/**
  * A RowTransformationPipeline takes a DataFrame and applies a Pipeline
  * of row-level transformations to return a new DataFrame.
  *
  * The supplied DataFrame and TransformationContext are provided as inputs
  * to the Pipeline.
  *
  * Created by markmo on 12/12/2015.
  */
class RowTransformationPipeline(private var nm: String) extends TableTransformation with Pipeline {

  val name = nm

  val transformations = mutable.Set[RowTransformation]()

  def apply(df: DataFrame, ctx: TransformationContext): DataFrame = {
    // loop through and execute transformations
    val results = df.rdd.map {
      sortedTransformations.foldLeft(_)((r, t) => t(r, ctx))
    }

    val schema = ctx(RowTransformation.SCHEMA_KEY).asInstanceOf[StructType]

    // update the schema for appended columns
    val newSchema =
      sortedTransformations
        .filter(_.isInstanceOf[AppendColumnRowTransformation])
        .foldLeft(schema)({
          case (s, t: AppendColumnRowTransformation) => s.add(t.meta)
        })

    // create a new DataFrame with a potentially updated schema
    val sqlContext = df.sqlContext
    sqlContext.createDataFrame(results, newSchema)
  }

  def addTransformations(transformations: RowTransformation*) {
    this.transformations ++= transformations
  }

  private def sortedTransformations = {
    // combine all the dependencies as a set of edges
    val edges = transformations.foldLeft(Traversable[(RowTransformation, RowTransformation)]()) { (a, b) => a ++ b.edges }

    // sort transformations by topological order
    val sorted: Iterable[RowTransformation] = tsort(edges)

    // include transformations with no dependencies
    val orphans = transformations -- sorted.toSet
    sorted ++ orphans
  }

}