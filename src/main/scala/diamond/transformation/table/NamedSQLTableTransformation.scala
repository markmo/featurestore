package diamond.transformation.table

import diamond.transformation.TransformationContext
import diamond.transformation.sql.SQLLoader
import org.apache.spark.sql.DataFrame

/**
  * Uses Spark SQL given a named query (queryName) from a configuration file
  * (propsPath) to construct a new DataFrame. The new DataFrame may be
  * computed with reference to the existing DataFrame, e.g. projection, and
  * to any values in the TransformationContext.
  *
  * Created by markmo on 16/12/2015.
  */
trait NamedSQLTableTransformation extends TableTransformation {

  import diamond.transformation.functions._

  val tableName: String

  val propsPath: String

  val queryName: String

  def apply(df: DataFrame, ctx: TransformationContext): DataFrame = {
    val sqlMap = SQLLoader.load(propsPath)
    val params = ctx("sqlparams").asInstanceOf[Map[String, String]]
    df.sqlContext.sql(sqlMap(name).template(params))
  }

}

object NamedSQLTableTransformation {

  def apply(name: String,
            tableName: String,
            propsPath: String,
            queryName: String
           )(op: (DataFrame, TransformationContext) => DataFrame) = {
    val myName = name
    val myTableName = tableName
    val myPropsPath = propsPath
    val myQueryName = queryName

    new NamedSQLTableTransformation {

      val name = myName

      val tableName = myTableName

      val propsPath = myPropsPath

      val queryName = myQueryName

      def append(df: DataFrame, ctx: TransformationContext) = op(df, ctx)

    }
  }

}