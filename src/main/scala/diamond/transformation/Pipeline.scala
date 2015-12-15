package diamond.transformation

import com.github.mdr.ascii.layout.{Graph, Layouter}
import diamond.transformation.utilityFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable

/**
  * Created by markmo on 12/12/2015.
  */
class Pipeline {

  val transformations = mutable.Set[Transformation]()

  def apply(df: DataFrame, ctx: TransformationContext): RDD[Row] = {
    // combine all the dependencies as a set of edges
    val edges = transformations.foldLeft(Traversable[(Transformation, Transformation)]()) { (a, b) => a ++ b.edges }

    // sort transformations by topological order
    val sorted: Iterable[Transformation] = tsort(edges)

    // include transformations with no dependencies
    val orphans = transformations -- sorted.toSet
    val all = (sorted ++ orphans).toList

    //all.map(_.name).foreach(println)

    // loop through and execute transformations
    df.rdd.map {
      all.foldLeft(_)((r, t) => {
//        println("Before: " + r)
//        println(">>> calling " + t.name)
//        val a = t(r, ctx)
//        println("After: " + a)
//        a
        t(r, ctx)
      })
    }
  }

  def addTransformations(transformations: Transformation*) {
    this.transformations ++= transformations
  }

  /**
    * Prints ASCII-art diagram of Directed Acyclic Graph (DAG).
    *
    * @return String
    */
  def printDAG() = {
    val vertices = transformations.map(_.name)
    val edges = transformations.foldLeft(Map[String, String]()) { (a, b) =>
      a ++ b.edges().map {
        case (from, to) => from.name -> to.name
      }
    }
    val graph = Graph(vertices.toList, edges.toList)
    Layouter.renderGraph(graph)
  }

}
