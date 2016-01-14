package diamond.transformation

import com.github.mdr.ascii.layout.{Graph, Layouter}
import diamond.io.{Sink, Source}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
  * Created by markmo on 19/12/2015.
  */
trait Pipeline {

  val name: String

  val transformations: mutable.Set[_ <: Transformation]

  def apply(df: DataFrame, ctx: TransformationContext): DataFrame

  def run(source: Source, sink: Sink, ctx: TransformationContext): DataFrame =
    sink(apply(source(ctx), ctx), ctx)

  def run(source1: Source, source2: Source, joinKeys1: List[String], joinKeys2: List[String], sink: Sink, ctx: TransformationContext): DataFrame = {
    val df = source1(ctx).join(source2(ctx))
    for ((joinKey1, joinKey2) <- joinKeys1.zip(joinKeys2)) {
      df.where(s"$joinKey1 = $joinKey2")
    }
    sink(apply(df, ctx), ctx)
  }

  /**
    * Prints ASCII-art diagram of Directed Acyclic Graph (DAG).
    *
    * @return String
    */
  def printDAG() = {
    val vertices = transformations.map(_.name)
    val edges = transformations.foldLeft(Map[String, String]()) { (a, b) =>
      a ++ b.edges.map {
        case (from, to) => from.name -> to.name
      }
    }
    val graph = Graph(vertices.toList, edges.toList)
    Layouter.renderGraph(graph)
  }

}
