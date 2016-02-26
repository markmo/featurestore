package diamond.transform.table

import diamond.transform.TransformationContext
import diamond.utility.stringFunctions
import org.apache.spark.sql.DataFrame

import scala.io.Source

/**
  * Uses Spark SQL given a SQL statement loaded from a configuration file
  * (filename) to construct a new DataFrame. The new DataFrame may be
  * computed with reference to the existing DataFrame, e.g. projection, and
  * to any values in the TransformationContext.
  *
  * Created by markmo on 16/12/2015.
  */
trait SQLFileTableTransformation extends TableTransformation {

  import stringFunctions._

  val tableName: String

  val filename: String

  def apply(df: DataFrame, ctx: TransformationContext): DataFrame = {
    val source = Source.fromFile(filename)
    val sql = try source.getLines().mkString("\n") finally source.close()
    val params = ctx("sqlparams").asInstanceOf[Map[String, String]]
    df.sqlContext.sql(sql.template(params))
  }

}

object SQLFileTableTransformation {

  def apply(name: String,
            tableName: String,
            filename: String
           )(op: (DataFrame, TransformationContext) => DataFrame) = {
    val myName = name
    val myTableName = tableName
    val myFilename = filename

    new SQLFileTableTransformation {

      val name = myName

      val tableName = myTableName

      val filename = myFilename

      def append(df: DataFrame, ctx: TransformationContext) = op(df, ctx)

    }
  }

}