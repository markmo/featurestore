package diamond.transformation.table

import java.util.Calendar

import diamond.models.JobStep
import diamond.transformation.utilityFunctions._
import diamond.transformation.{Pipeline, TransformationContext}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
  * Created by markmo on 16/12/2015.
  */
class TableTransformationPipeline(private var nm: String) extends Pipeline {

  val name = nm

  val transformations = mutable.Set[TableTransformation]()

  def apply(df: DataFrame, ctx: TransformationContext): DataFrame =
    sortedTransformations.foldLeft(df)((d, t) =>
      try {
        val newDF = t(d, ctx)
        val cal = Calendar.getInstance
        val steps = ctx("steps").asInstanceOf[List[JobStep]]
        ctx("steps", steps :+ JobStep(name, "Success", cal.getTime))
        newDF
      } catch {
        case e: Throwable =>
          val cal = Calendar.getInstance
          val steps = ctx("steps").asInstanceOf[List[JobStep]]
          ctx("steps", steps :+ JobStep(name, "Fail", cal.getTime, Some(e.getMessage)))
          throw e
      })

  def addTransformations(transformations: TableTransformation*) {
    this.transformations ++= transformations
  }

  private def sortedTransformations = {
    // combine all the dependencies as a set of edges
    val edges = transformations.foldLeft(Traversable[(TableTransformation, TableTransformation)]()) { (a, b) => a ++ b.edges }

    // sort transformations by topological order
    val sorted: Iterable[TableTransformation] = tsort(edges)

    // include transformations with no dependencies
    val orphans = transformations -- sorted.toSet
    sorted ++ orphans
  }

}
