package diamond.transformation.table

import diamond.transformation.TransformationContext
import org.apache.spark.sql.DataFrame

/**
  * Uses Spark SQL given a query string (sql) to construct a new DataFrame.
  *
  * The new DataFrame may be computed with reference to the existing DataFrame,
  * e.g. projection, and to any values in the TransformationContext.
  *
  * Created by markmo on 16/12/2015.
  */
trait SQLTableTransformation extends TableTransformation {

  val tableName: String

  val sql: String

  def apply(df: DataFrame, ctx: TransformationContext): DataFrame = {
    df.registerTempTable(tableName)
    df.sqlContext.sql(sql)
  }

}

object SQLTableTransformation {

  def apply(name: String,
            tableName: String,
            sql: String
           )(op: (DataFrame, TransformationContext) => DataFrame) = {
    val myName = name
    val myTableName = tableName
    val mySql = sql

    new SQLTableTransformation {

      val name = myName

      val tableName = myTableName

      val sql = mySql

      def append(df: DataFrame, ctx: TransformationContext) = op(df, ctx)

    }
  }

}