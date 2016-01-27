package diamond.transform.sql

import diamond.utility.functions
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.io.Source

/**
  * Load the SQL from a file
  *
  * Created by markmo on 16/12/2015.
  */
class SQLFileTransformation(filename: String, params: Map[String, String]) extends Serializable {

  import functions._

  def apply(sqlContext: SQLContext): DataFrame = {
    val source = Source.fromFile(filename)
    val sql = try source.getLines().mkString("\n") finally source.close()
    sqlContext.sql(sql.template(params))
  }

}
