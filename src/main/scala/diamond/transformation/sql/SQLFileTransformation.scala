package diamond.transformation.sql

import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.io.Source

/**
  * Load the SQL from a file
  *
  * Created by markmo on 16/12/2015.
  */
class SQLFileTransformation(filename: String) extends Serializable {

  def apply(sqlContext: SQLContext): DataFrame = {
    val source = Source.fromFile(filename)
    val sql = try source.getLines().mkString("\n") finally source.close()
    sqlContext.sql(sql)
  }

}
