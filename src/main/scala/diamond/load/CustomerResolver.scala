package diamond.load

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.hashing.MurmurHash3

/**
  * Created by markmo on 10/02/2016.
  */
class CustomerResolver(implicit val dataLoader: DataLoader) extends Serializable {

  /**
    *
    * @param df DataFrame mapping
    * @return RDD[(VertexId, (entity_id, id_type)]
    */
  def vertices(df: DataFrame): RDD[(VertexId, (String, String))] = {
    import df.sqlContext.implicits._

    df.select($"entity_id_1", $"entity_id_2", $"id_type_1", $"id_type_2")
      .flatMap(x => Iterable((x(0).toString, x(2).toString), (x(1).toString, x(3).toString)))
      .distinct()
      .map(x => (vertexId(x._1), x))
  }

  def edges(df: DataFrame): RDD[Edge[Double]] = {
    import df.sqlContext.implicits._

    df.select($"entity_id_1", $"entity_id_2", $"confidence")
      .map(x => Edge(vertexId(x(0).toString), vertexId(x(1).toString), x(2).asInstanceOf[Double]))
  }

  def makeGraph(vs: RDD[(VertexId, (String, String))], es: RDD[Edge[Double]]): Graph[(String, String), Double] = {
    val defaultEntity = ("Missing", "Missing")
    Graph(vs, es, defaultEntity)
  }

  def mapEntities(df: DataFrame,
                  entityType: String,
                  targetIdType: String,
                  confidenceThreshold: Double = 1.0,
                  mappingTable: Option[String] = None) = {

    val sqlContext = df.sqlContext
    val sc = sqlContext.sparkContext
    val mapping = dataLoader.readCurrentMapping(df.sqlContext, entityType, mappingTable)
    val graph = makeGraph(vertices(mapping), edges(mapping))
    graph.cache()

    //val bGraph = sc.broadcast(graph)
    //val findTargetIdUDF = udf(findTargetId(bGraph, _: String, targetIdType, confidenceThreshold))
    //df.withColumn(targetIdType, findTargetIdUDF($"entity_id"))

    // collecting as not working in parallel
    val mapped = df.collect().map(row =>
      Row.fromSeq(findTargetId(graph, row.getAs[String]("entity_id"), targetIdType, confidenceThreshold) +: row.toSeq)
    )

    val newSchema = StructType(StructField(targetIdType, StringType) +: df.schema)
    sqlContext.createDataFrame(sc.parallelize(mapped), newSchema)
  }

  // TODO
  // not sure if a graph lookup approach will work
  def findTargetId(graph: Graph[(String, String), Double],
                   srcEntityId: String,
                   targetIdType: String,
                   confidenceThreshold: Double = 1.0): String = {

    if (graph.vertices == null) throw sys.error("Empty Graph!!")

    // following is wrong
    val result = graph.pregel("Missing", maxIterations = 20, activeDirection = EdgeDirection.Either)(
      // (VertexId, VD, A) => VD
      (id, dist, newDist) => dist,

      // triplet:
      // ((srcId, srcAttr), (dstId, dstAttr), attr)
      // ((srcVertexId, (srcEntityId, srcIdType)), (dstVertexId, (dstEntityId, dstIdType)), confidence)
      triplet => {
        if (triplet.dstAttr._2 == targetIdType && triplet.attr >= confidenceThreshold) {
          Iterator.empty
        } else {
          Iterator((triplet.dstId, triplet.dstAttr._1))
        }
      },

      // (A, A) => A
      (a, b) => b
    )
    result.vertices.take(1)(0)._2._1
  }

  def vertexId(str: String): VertexId = MurmurHash3.stringHash(str).toLong

}
